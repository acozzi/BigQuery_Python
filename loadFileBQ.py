# Docs 
#   https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html
#   https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
#   https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
#   https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv

## Import Libraries BEGIN ##
from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google_auth_oauthlib import flow
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
import csv
import json
## Import Libraries END ## 

## Authentication Process BEGIN ##
launch_browser = True
creds = None
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

# token.json is the local file created by this script with the auth scope. Delete this file to refresh the auth scopes.
if os.path.exists('token.json'):
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
# Download client_secret.json file from https://console.cloud.google.com/apis/credentials
# It contains sensible information for the auth
    else:
        appflow = flow.InstalledAppFlow.from_client_secrets_file(
    "client_secret.json", scopes=SCOPES)
        creds = appflow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())    
## Authentication Process END ##

## API BEGIN ##
# data.json is an ad-hoc created file to keep some info data like project name, dataset name or table name
# Seems like:
"""
{
    "project": "your_prject_name",
    "dataset": "your_dataset_name",
    "table": "destination_table_name",
    "csvSource": "ingest_file.csv"
}
"""

with open('data.json') as json_file:
    data = json.load(json_file)

credentials = creds 
project = data['project']

# create a BQ client
client = bigquery.Client(project=project, credentials=credentials)

# define table name (concatenate dataset.tableName)
table_id = data['dataset'] + '.' + data['table']

# Read the first row in your .csv file to create the schema.
# If the .csv file lacks header, avoid this step
# Check the arg "delimiter", you can change it if necesary for comma "," space " " or anything.
reader = csv.reader(
    open(data['csvSource'], mode='r', encoding='utf-8-sig'), delimiter=";")
head = next(reader)

# Create an array of SchemaField objects with the data from the first .csv row
schemaRaw = []
for field in head:
    schemaRaw.append(bigquery.SchemaField(field, "STRING", mode="NULLABLE"))

# Configure the load job
job_config = bigquery.LoadJobConfig(
    schema=schemaRaw, # load the schema created a few lines above
    skip_leading_rows=1, # Skip the first row which is the header.  
    source_format=bigquery.SourceFormat.CSV, # Define the source format
)
job_config.field_delimiter = ';' # Define the delimiter as above

# Open the source file and launch the job
with open(data['csvSource'], mode='rb') as fp:
    
    load_job = client.load_table_from_file(
        fp, table_id, job_config=job_config
    )

    load_job.result()  
    
    # Define the destination table
    destination_table = client.get_table(table_id)  
    # Print the resuls
    print("Loaded {} rows.".format(destination_table.num_rows))
