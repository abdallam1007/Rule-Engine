import json

table = "drivers_order_response" # ! hardcoded for now

# deletes all rows from the PostgresSQL table
def delete_all_rows(user_db):
    sql = f"DELETE FROM {table}"
    user_db.execute(sql)

# creates a sql query based on json data structure for a row
def construct_query(update):
    columns = ", ".join(update.keys())
    value_placeholders = ", ".join(["%s"] * len(update))

    query = f"INSERT INTO {table} ({columns}) VALUES ({value_placeholders})"
    return query

# given the round number, read the list of new updatees and create postgresSQL rows
def insert_new_updates(user_db, round, updates_fp):
    f = open(updates_fp, 'r')
    data = json.load(f)
    f.close()

    # Get the updates for current round number
    updates = data.get(str(round))
    # for every update, insert a corresponding database row
    for update in updates:
        # construct sql query to insert row in postgres db
        query = construct_query(update)
        row_data = tuple(update.values())
        user_db.execute(query, row_data)


def update_database(user_db, round, updates_fp):
    delete_all_rows(user_db)
    insert_new_updates(user_db, round, updates_fp)