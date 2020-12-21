import io
from google.cloud import bigquery
from google.oauth2 import id_token
import google.auth
from google.auth.transport.requests import Request
import requests
from flask import escape


def query(url_query, audience, method='GET', body=None):
    open_id_connect_token = id_token.fetch_id_token(Request(),
                                                    audience=audience)

    resp = requests.request(
        method,
        url_query,
        headers={'Authorization': 'Bearer {}'.format(open_id_connect_token)},
        json=body)

    return resp.text

def accumulation_import(request):
    # Construct a BigQuery client object.
    PROJECT_ID = 'thermostat-292016'
    DATASET_ID = 'thermostat'
    TABLE_ID = 'metric'
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("dt", "TIMESTAMP"),
            bigquery.SchemaField("temp_basement", "FLOAT64"),
            bigquery.SchemaField("temperature", "FLOAT64"),
            bigquery.SchemaField("humidity", "FLOAT64"),
            bigquery.SchemaField("stove_exhaust_temp", "FLOAT64"),
            bigquery.SchemaField("motion", "BOOL"),
        ],
        time_partitioning=bigquery.TimePartitioning(field="dt"),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        maxBadRecords=0)


    audience = "https://thermostat-agent-ppb6otnevq-uk.a.run.app/metric/accumulate/"
    uri = audience + "?load=4&records=True"

    data_load = query(uri, audience)
    data_as_file = io.StringIO(data_load, newline='\n')

    print(data_load)

    load_job = client.load_table_from_file(
        data_as_file,
        table_ref,
        location="US-EAST4",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.running()
    errors = load_job.errors
    result = load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))

    return "Loaded {} rows.".format(destination_table.num_rows)
