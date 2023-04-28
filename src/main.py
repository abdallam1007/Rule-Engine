import argparse
import pymongo

import scheduler
import rule_engine


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-rfp', default= "data/rules.json", type= str, help='path to def of rules in json format')
    args = parser.parse_args()

    mongoDB_conn = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongoDB_conn["rule_engine_db"]

    rules_fp = args.rfp
    rules = rule_engine.create_rules(mongo_db, rules_fp)


    scheduler.init_scheduler(mongo_db, rules)

    mongoDB_conn.close()


if __name__ == '__main__':
    main()