# Docs 
#   https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html
#   https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
#   https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
#   https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google_auth_oauthlib import flow
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
import csv
import json


launch_browser = True
creds = None
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

if os.path.exists('token.json'):
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        appflow = flow.InstalledAppFlow.from_client_secrets_file(
    "client_secret.json", scopes=SCOPES)
        creds = appflow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())    


with open('data.json') as json_file:
    data = json.load(json_file)

credentials = creds 
project = data['project']

client = bigquery.Client(project=project, credentials=credentials)


table_id = data['dataset'] + '.' + data['table']


reader = csv.reader(
    open(data['csvSource'], mode='r', encoding='utf-8-sig'), delimiter=";")
head = next(reader)

schemaRaw = []
for field in head:
    schemaRaw.append(bigquery.SchemaField(field, "STRING", mode="NULLABLE"))


job_config = bigquery.LoadJobConfig(
    schema=schemaRaw,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
job_config.field_delimiter = ';'

with open(data['csvSource'], mode='rb') as fp:
    
    load_job = client.load_table_from_file(
        fp, table_id, job_config=job_config
    )

    load_job.result()  

    destination_table = client.get_table(table_id)  
    print("Loaded {} rows.".format(destination_table.num_rows))