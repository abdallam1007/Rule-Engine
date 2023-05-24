import json
import psycopg2

# * IMPORTANT NOTE
'''
This file is only created as a temporary component to simulate a 
database that gets updated with some information about the users
that can be used to detect violations

Everything related to this component should be deleted upon connecting
a real live database to this code
'''

#----------------------- HELPER FUNCTIONS -----------------------#

def connect_database(rule):

    # establishing a connection with the database
    db_connection_str = rule["db_connection_str"]
    conn = psycopg2.connect(db_connection_str)

    # enable auto commiting of the changes
    conn.autocommit = True

    return conn


def close_database(conn, user_db):
    
    # close the connection with the database
    user_db.close()
    conn.close()


def construct_query(update, table):

    # creates a sql query based on json data structure for a row
    columns = ", ".join(update.keys())
    value_placeholders = ", ".join(["%s"] * len(update))

    query = f"INSERT INTO {table} ({columns}) VALUES ({value_placeholders})"
    return query

#----------------------------------------------------------------#

def delete_all_rows(rule):

    conn = connect_database(rule)
    user_db = conn.cursor()

    # delete all the rows from the database
    table = rule["table_name"]
    sql = f"DELETE FROM {table}"
    user_db.execute(sql)

    close_database(conn, user_db)


def insert_new_updates(rule):

    conn = connect_database(rule)
    user_db = conn.cursor()

    # unpack the rule to get the needed information
    updates_fp = rule["updates_fp"]
    table = rule["table_name"]
    round = rule["round"]

    # read the json file in a python dictionary
    f = open(updates_fp, 'r')
    data = json.load(f)
    f.close()

    if round >= len(data):
        return

    # get the updates to be inserted in the database for a specific round
    updates = data[round]

    # for every update, insert a corresponding database row
    for update in updates:
        # construct sql query to insert row in postgres db
        query = construct_query(update, table)
        row_data = tuple(update.values())
        user_db.execute(query, row_data)

    close_database(conn, user_db)


def update_database(rule):

    # go to the next round
    rule["round"] += 1

    # delete the database to add the new updates
    delete_all_rows(rule)
    insert_new_updates(rule)