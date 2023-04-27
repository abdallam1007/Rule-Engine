import rule_engine
import database_engine

def scheduler(mongo_db, user_db, rule):

    # Todo:
    # - loop and sleep every crone, keep track of the rule round
    # - call functoin apply_rule_on_database from rule_engine in each iteration
    # - call function prcoess_rule_by_row
    # - call update_database from database engine

    pass


# * Notes:
# * This code works only for one rule, to make it work for multiple rules:
#       * change the round to be part of the rule
#       * instead of looping figure out how to use a crone