from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google_auth_oauthlib import flow
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.cloud import bigquery

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
    "gcpUser.json", scopes=SCOPES)
        creds = appflow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())    

credentials = creds 
project = 'name_of_your_proyect'
client = bigquery.Client(project=project, credentials=credentials)
query_string = 

"""
    SELECT name, SUM(number) as total
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE state = 'NY'
    GROUP BY name
    ORDER BY total DESC
    LIMIT 50 ;
"""

query_job = client.query(query_string)

for row in query_job.result():
    print("{}: {}".format(row["name"], row["total"]))
    
