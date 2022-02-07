# BigQuery_Python
## Some ways to work with BQ from a Python desktop

The file baseQuery.py is similar to the provided by google in https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
But in his case, you need to retake the credentials from oAuth2 in every run from your code.

Here I find the way to save the credentials in a token file and only retake if one of the scopes was changed or the credential was expired for any reason.


Dependencies:
pip install --upgrade google-cloud-bigquery
it will install:
[
    cachetools-5.0.0 
    google-api-core-2.5.0 
    google-auth-2.6.0 
    google-cloud-bigquery-2.32.0 
    google-cloud-core-2.2.2 
    google-crc32c-1.3.0 
    google-resumable-media-2.1.0 
    googleapis-common-protos-1.54.0 
    grpcio-1.43.0 
    grpcio-status-1.43.0 
    proto-plus-1.19.9 
    protobuf-3.19.4 
    pyasn1-0.4.8 
    pyasn1-modules-0.2.8 
    python-dateutil-2.8.2 
    rsa-4.8
]
pip install --upgrade google-cloud-bigquery-storage

Download Credentials from
https://console.cloud.google.com/apis/dashboard
Credentials - OAuth 2.0 Client Id
Add json file to your system path. (make sure that gitignore file include *.json files)


