import json

# create the rule document from json and add it to the Rules collection
def create_rule(mongo_db, rule_filepath):
    try:
        f = open(rule_filepath, 'r')
        rule = json.load(f)
        f.close()

        rule_obj = mongo_db['Rules'].insert_one(rule)
        return rule_obj

    except OSError:
        raise "OSError: Couldn't open the rule file\n"

# apply the sql query on the postgresSQL database and return the matches
def apply_rule_on_database(user_db, sql_query):
    user_db.execute(sql_query)
    matches = user_db.fetchall()
    return matches

def process_rule_matches_by_row(mongo_db, rows, rule): # version of proces_rule_matches
    # Todo:
    # - check if a valid match with DDT and store it
    # - update the sev level and take actions and store action log
    pass




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