import pandas as pd

from google.cloud import bigquery
#from colorama import Fore, Style
from pathlib import Path

from rss_scrapper.params import *

def data_test():
    return "test success"

def get_data_from_bq(query):
    #full_table_name = f"{GCP_PROJECT_SEBT84}.{BQ_DATASET}.raw_{TABLE}"

    client = bigquery.Client(project=GCP_PROJECT)
    query_job = client.query(query)
    results = query_job.result()  # Waits for job to complete.
    df = results.to_dataframe()
    #print(f"✅ Data downloaded from bigquery")

    return df

def get_rss_feed_list():
    TABLE = 'rss_feed_list'

    query = f"""
            SELECT
                *
            FROM {GCP_PROJECT}.{BQ_DATASET}.{TABLE}"""

    df = get_data_from_bq(query)

    print(f"✅ {TABLE} Data downloaded from bigquery with shape {df.shape}\n")

    return df

def load_data_bq(df: pd.DataFrame
                 , replace: bool
                , TABLE: str):
    full_table_name = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLE}"

    client = bigquery.Client()

    write_mode = "WRITE_TRUNCATE" if replace else "WRITE_APPEND"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)

    job = client.load_table_from_dataframe(df, full_table_name, job_config=job_config)
    result = job.result()

    print(f"✅ Data saved to bigquery, table: {TABLE} with shape {df.shape}\n")

def get_latest_articles_from_bq(cut_off_date_str):
    TABLE = 'rss_feed_articles'

    query = f"""
            SELECT
                *
            FROM {GCP_PROJECT}.{BQ_DATASET}.{TABLE}
            where pubDate >= '{cut_off_date_str}'"""

    df = get_data_from_bq(query)

    print(f"✅ {TABLE} Data downloaded from bigquery with shape {df.shape}\n")

    return df
