import argparse
import psycopg2
import pymongo

import rule_engine
import scheduler

def postgres_connect(db_connection_str):
    # establishing the connection
    conn = psycopg2.connect(db_connection_str)
    conn.autocommit = True
    return conn

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-rfp', default= "data/rules.json", type= str, help='path to def of rules in json format')
    args = parser.parse_args()

    mongoDB_conn = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongoDB_conn["rule_engine_db"]

    rules_fp = args.rfp
    rules = rule_engine.create_rules(mongo_db, rules_fp)

    #db_connection_str = rule_obj["db_connection_str"]
    #userDB_conn = postgres_connect(db_connection_str)
    #user_db = userDB_conn.cursor()

    scheduler.init_scheduler(mongo_db, rules)

    mongoDB_conn.close()
    #user_db.close()
    #userDB_conn.close()


if __name__ == '__main__':
    main()