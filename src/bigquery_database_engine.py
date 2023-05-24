import json
from google.cloud import bigquery

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
    project_id = rule["db_project_id"]
    client = bigquery.Client(project=project_id)

    return client


def close_database(user_db):

    # close the connection with the database
    user_db.close()


#----------------------------------------------------------------#

def delete_all_rows(rule):

    user_db = connect_database(rule)

    # delete all the rows from the database
    table_name = rule["table_name"]
    dataset_id = rule["dataset_id"]

    table_ref = user_db.dataset(dataset_id).table(table_name)
    user_db.delete_rows(table_ref, "TRUE")

    close_database(user_db)


def insert_new_updates(rule):

    user_db = connect_database(rule)

    # unpack the rule to get the needed information
    updates_fp = rule["updates_fp"]
    round = rule["round"]

    # read the json file in a python dictionary
    f = open(updates_fp, 'r')
    data = json.load(f)
    f.close()

    if round >= len(data):
        return

    dataset_id = rule["dataset_id"]
    table_name = rule["table_name"]
    table_ref = user_db.dataset(dataset_id).table(table_name)
    table = user_db.get_table(table_ref)
    schema = table.schema

    # get the updates to be inserted in the database for a specific round
    updates = data[round]
    # for every update, insert a corresponding database row
    #user_db.insert_rows(table_ref, rows=updates)
    user_db.insert_rows_json(table_ref, json_rows=updates)
    close_database(user_db)


def update_database(rule):

    # go to the next round
    rule["round"] += 1

    # delete the database to add the new updates
    delete_all_rows(rule)
    insert_new_updates(rule)