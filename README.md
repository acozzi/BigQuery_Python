# BigQuery_Python
## Some ways to work with BQ from a Python desktop

The file baseQuery.py is similar to the provided by google in https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
But in his case, you need to retake the credentials from oAuth2 in every run from your code.

Here I find the way to save the credentials in a token file and only retake if one of the scopes was changed or the credential was expired for any reason.
