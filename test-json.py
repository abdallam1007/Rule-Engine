import json

table = "drivers_order_response" # ! hardcoded for now

# creates a sql query based on json data structure for a row
def construct_insert_row_query(row):
    columns = ", ".join(row.keys())
    value_placeholders = ", ".join(["%s"] * len(row))

    query = f"INSERT INTO {table} ({columns}) VALUES ({value_placeholders})"
    return query


# given the round number, read the list of new updatees and create postgresSQL rows
def insert_new_updates(user_db, round, updates_fp):
    f = open(updates_fp, 'r')
    data = json.load(f)
    f.close()

    # Get the updates for current round number
    updates = data.get(str(round))
    query = None
    # for every update, insert a corresponding database row
    for row in updates:
        row_data = json.dumps(row)

        # construct sql query based on first update structure
        if not query:
            insert_row_query = construct_insert_row_query(row)

        row_data = tuple(row.values())
        print(insert_row_query, row_data)
        # user_db.execute(insert_row_query, (row_data,))

insert_new_updates(None, 1, "data/updates_DDO.json")