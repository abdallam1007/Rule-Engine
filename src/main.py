import argparse
import psycopg2
import pymongo

import rule_engine
import scheduler

def postgres_connect(db_name):
    # establishing the connection
    conn = psycopg2.connect(
    database=db_name, user='postgres', password='password', host='127.0.0.1', port= '5432')
    conn.autocommit = True

    # creating a cursor object using the cursor() method
    user_db = conn.cursor()
    return user_db

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-fp', default= "data/rule_DDO.json", type= str, help='path to def of rule in json format')
    args = parser.parse_args()

    mongo_db = pymongo.MongoClient("mongodb://localhost:27017/")["rule_engine_db"]

    # Todo: 
    # - connect to the the mongoDB database
    # - call create_rule from rule_engine
    # - call rule_scheduler from rule_scheduler

    rule_filepath = args.fp
    rule_obj = rule_engine.create_rule(mongo_db, rule_filepath)

    user_db = postgres_connect(rule_obj["db_name"])
    scheduler.scheduler(mongo_db, user_db, rule_obj)

    pass

if __name__ == '__main__':
    main()
