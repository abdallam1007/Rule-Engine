import json

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

# apply the sql query on the postgresSQL database and return the matches
def apply_rule(mongo_db, rule):
    #user_db.execute(sql_query)
    #matches = user_db.fetchall()


    # Todo:
    # - connect to the user_db and apply the rule to get the "matches"
    # - call the fuction process_rule_matches_by_row
    # - call the update_database function with the rule
    #   - update_database modifies round in python_obj/db_obj rule
    pass

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