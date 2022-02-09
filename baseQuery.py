## Import Libraries BEGIN ##
from __future__ import print_function
import os.path
import json
from google.auth.transport.requests import Request
from google_auth_oauthlib import flow
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
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
    else:
# Download client_secret.json file from https://console.cloud.google.com/apis/credentials
# It contains sensible information for the auth
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

# Create a BQ client
client = bigquery.Client(project=project, credentials=credentials)

# Write your StandarSQL query
query_string = """
    SELECT name, SUM(number) as total
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE state = 'NY'
    GROUP BY name
    ORDER BY total DESC
    LIMIT 50 ;
"""
# Load the job
query_job = client.query(query_string)

# Print/manage results
# Row iterator reference https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.RowIterator
for row in query_job.result():
    print("{}: {}".format(row["name"], row["total"]))