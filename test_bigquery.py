from google.cloud import bigquery

# Create a BigQuery client
client = bigquery.Client()

# Specify the project ID and dataset ID
project_id = "dry-run-db"
dataset_id = "dry_run_db"

# Get a reference to the dataset
dataset_ref = client.dataset(dataset_id, project=project_id)
dataset = client.get_dataset(dataset_ref)

# Perform operations on the dataset
# For example, list tables in the dataset
tables = client.list_tables(dataset)
for table in tables:
    print(table.table_id)

# pip install google-cloud-bigquery
