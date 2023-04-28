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
    parser.add_argument('-fp', default= "data/rule_DDO.json", type= str, help='path to def of rule in json format')
    args = parser.parse_args()

    mongo_db = pymongo.MongoClient("mongodb://localhost:27017/")["rule_engine_db"]
    rule_filepath = args.fp
    rule_obj = rule_engine.create_rule(mongo_db, rule_filepath)

    conn = postgres_connect(rule_obj["db_connection_str"])
    user_db = conn.cursor()
    scheduler.scheduler(mongo_db, user_db, rule_obj)

    mongo_db.close()
    user_db.close()
    conn.close()

    pass

if __name__ == '__main__':
    main()
