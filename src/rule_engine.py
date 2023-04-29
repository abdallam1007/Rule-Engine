import json
from datetime import datetime, timedelta

import database_engine


#----------------------- HELPER FUNCTIONS -----------------------#

def get_last_match(mongo_db, row):
    matches_collection = mongo_db["Matches"]
    username = row[1]

    last_match = matches_collection.find_one({ 'username': username },
                                sort=[('_id', -1)])
    
    return last_match

def is_valid_match(mongo_db, rule, row):
    last_match = get_last_match(mongo_db, row)
    if last_match is not None:
        # check the DDT
        DDT = rule["DDT"]
        last_match_timestamp = last_match["timestamp"]
        curr_timestamp = datetime.utcnow()

        delta = curr_timestamp - last_match_timestamp
        delta_secs = delta.total_seconds()
        return delta_secs >= DDT
    
    return True

def update_sev_level(mongo_db, rule, row):
    last_match = get_last_match(mongo_db, row)
    if last_match is not None:
        print("There was a previous match")
        curr_sev_level = last_match["sev_level"]
        sev = rule["severity_configs"]["severities"][curr_sev_level]
        sev_exp_period = sev["exp_period"]

        last_match_timestamp = last_match["timestamp"]
        curr_timestamp = datetime.utcnow()

        expiry_timestamp = last_match_timestamp + timedelta(seconds=sev_exp_period)

        if curr_timestamp < expiry_timestamp:
            print("The expiration period didn't expire")
            #  didn't expire
            return curr_sev_level + 1
        
    return 0 # exp passed or it is the first match

#----------------------------------------------------------------#

# create the rules documents from json and add them to the Rules collection
def create_rules(mongo_db, rules_fp):
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

# applies the rule
def apply_rule(mongo_db, rule):
    conn = database_engine.connect_database(rule)
    user_db = conn.cursor()
    
    sql_query = rule["sql_query"]
    user_db.execute(sql_query)
    rows = user_db.fetchall()

    print("-----------------------------------")
    print("Matched [" + str(len(rows)) + "] rows")
    
    process_rule_matches_by_row(mongo_db, rule, rows)
    print("-----------------------------------")
    database_engine.update_database(rule)

# version of proces_rule_matches
def process_rule_matches_by_row(mongo_db, rule, rows):
    for row in rows:
        if is_valid_match(mongo_db, rule, row):
            print("Found a valid match")
            new_sev_level = update_sev_level(mongo_db, rule, row)
            max_sev_level = len(rule["severity_configs"]["severities"]) - 1
            if new_sev_level > max_sev_level:
                print("Reached Final sev_level again")
                if not rule["severity_configs"]["re_apply"]:
                    print("No need to re_apply the rule")
                    return

                print("Need to re_apply the rule")
                new_sev_level = max_sev_level
               
            print("curr_sev_level is = " + str(new_sev_level))
            
            # create a match in the database
            create_match(mongo_db, rule, row, new_sev_level)
            
            # log and apply the actions
            log_actions(mongo_db, rule, row, new_sev_level)
            apply_actions(rule, new_sev_level)

def create_match(mongo_db, rule, row, sev_level):
    sev = rule["severity_configs"]["severities"][sev_level]
    sev_exp_period = sev["exp_period"]

    match = {
        # * user_id at 0
        "username": row[1], # ! hardcoded for readability
        "timestamp": datetime.utcnow(),
        "sev_level": sev_level,
        "rule": rule
    }

    matches_collection = mongo_db["Matches"]
    matches_collection.create_index("timestamp",
                        expireAfterSeconds=sev_exp_period)
    matches_collection.insert_one(match)

def log_actions(mongo_db, rule, row, sev_level):
    sev = rule["severity_configs"]["severities"][sev_level]

    action_log = {
        # * user_id at 0
        "username": row[1], # ! hardcoded for readability
        "timestamp": datetime.utcnow(),
        "severity_level": sev_level,
        "severity": sev,
        "rule": rule
    }

    action_logs_collection = mongo_db["Action Logs"]
    action_logs_collection.insert_one(action_log)

def apply_actions(rule, sev_level):
    sev = rule["severity_configs"]["severities"][sev_level]
    for action in sev["actions"]:
        print("Perform Action ==> " + action)

# ! <<<<    CAN'T DO THIS    >>>> !
# there is no user id, therefore there is no way to create a match
# therefore, there is no way to implement DDT or severity levels

'''
def process_rule_matches_by_condition(row, rule): # version of proces_rule_matches
    # Todo:
    # - take the value and apply condition
    # - if true, update sev levels and take actions

    pass
'''