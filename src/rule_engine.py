import json
from datetime import datetime, timedelta

import database_engine


#----------------------- HELPER FUNCTIONS -----------------------#

# Todo: filter by (username,rule) instead of just username
def get_last_match(mongo_db, rule, row):

    matches_collection = mongo_db["Matches"]
    username = row[1]

    # get the last match added for this user with this rule
    last_match = matches_collection.find_one({
        'username': username,
        'rule_id': rule["_id"]
    }, sort=[('_id', -1)])

    return last_match


# check if a match is valid by making sure the DDT time from
# a previous match has passed
def is_valid_match(mongo_db, rule, row):

    last_match = get_last_match(mongo_db, rule, row)

    # if there was a match before this, then maybe the DDT didn't pass
    if last_match is not None:
        DDT = rule["DDT"]
        last_match_timestamp = last_match["timestamp"]
        curr_timestamp = datetime.utcnow()

        delta = curr_timestamp - last_match_timestamp
        delta_secs = delta.total_seconds()
        return delta_secs >= DDT
    
    # if it's the first or only match, then it is valid
    return True


# update the sev_level based on if the previous severity
# expired or not yet
def update_sev_level(mongo_db, rule, row):

    last_match = get_last_match(mongo_db, rule, row)

    # if there was a prev match, check if we move to the next sev level
    if last_match is not None:
        print("There was a previous match")
        curr_sev_level = last_match["sev_level"]
        sev = rule["severity_configs"]["severities"][curr_sev_level]
        sev_exp_period = sev["exp_period"]

        last_match_timestamp = last_match["timestamp"]
        curr_timestamp = datetime.utcnow()

        expiry_timestamp = last_match_timestamp + timedelta(seconds=sev_exp_period)

        # if the prev match didn't expire, move to the next level
        if curr_timestamp < expiry_timestamp:
            print("The expiration period didn't expire")
            return curr_sev_level + 1
    
    # if there is no prev match, either they expired or this is the first
    return 0

#----------------------------------------------------------------#

# create the rules documents from json and add them to the Rules collection
def create_rules(mongo_db, rules_fp):

    # read the rules into a python list from json
    f = open(rules_fp, 'r')
    rules_json = json.load(f)
    f.close()

    rules_collection = mongo_db['Rules']
    rules_objs = []
    
    for rule in rules_json:
        # insert rule in the collection
        inserted_rule = rules_collection.insert_one(rule)
        
        # Retrieve the inserted document
        rule_obj = rules_collection.find_one({"_id": inserted_rule.inserted_id})
        rules_objs.append(rule_obj)
    
    return rules_objs


def apply_rule(mongo_db, rule):

    # connect to the user database
    conn = database_engine.connect_database(rule)
    user_db = conn.cursor()
    
    # apply the sql query
    sql_query = rule["sql_query"]
    user_db.execute(sql_query)
    rows = user_db.fetchall()

    print("-----------------------------------")
    print("Matched [" + str(len(rows)) + "] rows")
    
    process_rule_matches_by_row(mongo_db, rule, rows)
    print("-----------------------------------")

    # * The database has to be updated manually for now
    database_engine.update_database(rule)


def process_rule_matches_by_row(mongo_db, rule, rows):

    #process the rows one by one as apply_on indicated
    for row in rows:

        # check if this row should be considered to be a match
        if is_valid_match(mongo_db, rule, row):
            print("Found a valid match")

            # get the new sev level based on the exp period
            new_sev_level = update_sev_level(mongo_db, rule, row)
            max_sev_level = len(rule["severity_configs"]["severities"]) - 1

            # if the user matched again while being in the max sev level
            if new_sev_level > max_sev_level:
                print("Reached Final sev_level again")
                if not rule["severity_configs"]["re_apply"]:
                    print("No need to re_apply the rule")
                    return

                # keep sev level on max to re apply
                print("Need to re_apply the rule")
                new_sev_level = max_sev_level
               
            print("curr_sev_level is = " + str(new_sev_level))
            
            # create a match in the database
            create_match(mongo_db, rule, row, new_sev_level)
            
            # log the action and apply it
            log_actions(mongo_db, rule, row, new_sev_level)
            apply_actions(rule, new_sev_level)


def create_match(mongo_db, rule, row, sev_level):

    sev = rule["severity_configs"]["severities"][sev_level]
    sev_exp_period = sev["exp_period"]

    # create a match object
    match = {
        # * username here is supposed to be user_id, it is currently like this
        # * for readability purpoces.
        # * Howerver, for production this is changed to be user_id

        # * We enforce that each row returned by the database
        # * has to have user_id as the first column
        "username": row[1],
        "timestamp": datetime.utcnow(),
        "sev_level": sev_level,
        "rule_id": rule["_id"]
    }

    matches_collection = mongo_db["Matches"]
    
    # create an index on the timestamp to be the base for the TTL on the document
    matches_collection.create_index("timestamp",
                        expireAfterSeconds=sev_exp_period)
    matches_collection.insert_one(match)


def log_actions(mongo_db, rule, row, sev_level):
    
    sev = rule["severity_configs"]["severities"][sev_level]

    action_log = {
        # * username here is supposed to be user_id, it is currently like this
        # * for readability purpoces.
        # * Howerver, for production this is changed to be user_id

        # * We enforce that each row returned by the database
        # * has to have user_id as the first column
        "username": row[1],
        "timestamp": datetime.utcnow(),
        "severity_level": sev_level,
        "severity": sev,
        "rule": rule
    }

    action_logs_collection = mongo_db["Action Logs"]
    action_logs_collection.insert_one(action_log)

def apply_actions(rule, sev_level):

    # get the actions to perform from the severity
    sev = rule["severity_configs"]["severities"][sev_level]

    # Todo: create API calls to perform the actions
    # print all the actions
    for action in sev["actions"]:
        print("Perform Action ==> " + action)