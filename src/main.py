import argparse
import pymongo

import scheduler
import rule_engine


def main():
    
    # parser for the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-rfp', default= "data/rules.json", type= str, help='path to def of rules in json format')
    args = parser.parse_args()

    # connect to the mongo database used for running the code
    mongo_db_conn_str = "mongodb://localhost:27017/"
    mongoDB_conn = pymongo.MongoClient(mongo_db_conn_str)
    mongo_db = mongoDB_conn["rule_engine_db"]

    # start the rule_engine by creating the rules
    rules_fp = args.rfp
    rules = rule_engine.create_rules(mongo_db, rules_fp)

    # init the scheduler with the crons defined for each rule
    scheduler.init_scheduler(mongo_db, rules)

    mongoDB_conn.close()


if __name__ == '__main__':
    main()