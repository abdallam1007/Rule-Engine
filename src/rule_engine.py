import json

import database_engine


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

    process_rule_matches_by_row(mongo_db, rows, rule)
    database_engine.update_database(rule)

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