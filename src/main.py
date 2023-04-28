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

    mongoDB_conn = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongoDB_conn["rule_engine_db"]
    
    rule_filepath = args.fp
    rule_obj = rule_engine.create_rule(mongo_db, rule_filepath)

    db_connection_str = rule_obj["db_connection_str"]
    userDB_conn = postgres_connect(db_connection_str)
    user_db = userDB_conn.cursor()
    scheduler.scheduler(mongo_db, user_db, rule_obj)

    mongoDB_conn.close()
    user_db.close()
    userDB_conn.close()


if __name__ == '__main__':
    main()