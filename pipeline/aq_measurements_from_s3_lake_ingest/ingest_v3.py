'''
Ingest data on per year basis. 
Date range is only at year level granularity.
The fastest way to ingest big amounts of data.
Acts fast because requires only one subprocess(AWS CLI) initiation per year.
Best for the initial ingestion of data.

TODO: same algo with month level granularity.
'''

import subprocess
import pandas as pd
import os
import pathlib
import numpy as np

def urljoin(*args, ispath=True):
    """
    Joins given arguments into an url. Trailing but not leading slashes are
    stripped for each argument.
    """

    res = "/".join(map(lambda x: str(x).strip('/'), args))
    if ispath and len(res) > 0:
        if res[-1] != '/':
            res = res + '/'
            
    return res


gs_data_bucket = os.environ["AQ_DATA_BUCKET_URL"] 
ingest_from_year = os.environ["AQ_FROM_YEAR"]
ingest_from_month = '01' #os.environ["AQ_FROM_MO"]
ingest_to_year = os.environ["AQ_TO_YEAR"]
ingest_to_month = '12' #os.environ["AQ_TO_MO"]
ingest_country_name = os.environ["AQ_COUNTRY_NAME"]

# Local testing
#gs_data_bucket = 'gs://kestra-de-main-bucket/'
#ingest_from_year = '2024'
#ingest_from_month= '01'
#ingest_to_year = '2025'
#ingest_to_month = '03'
#ingest_country_name = 'Slovakia'

ingest_from_datetime_iso = f'{ingest_from_year}-{ingest_from_month}-01 00:00:00'

ingest_to_year_tmp = ingest_to_year 
ingest_to_month_tmp = ingest_to_month
if ingest_to_month == '12':
    ingest_to_year_tmp = f'{int(ingest_to_year)+1}'
    ingest_to_month_tmp = '01'
else:
    ingest_to_month_tmp =f'{int(ingest_to_month) + 1:02d}'
ingest_to_datetime_iso = f'{ingest_to_year_tmp}-{ingest_to_month_tmp}-01 00:00:00'

gs_raw_data_path =  '/aq/raw/'
gs_raw_data_path_url = urljoin(gs_data_bucket, gs_raw_data_path)
gs_prod_data_path_url = urljoin(gs_data_bucket, '/aq/data/')

print('URL: ', gs_raw_data_path_url)
gs_locations_path = urljoin(gs_raw_data_path_url, '/sensors_topology/locs/')
locations_df = pd.read_parquet(gs_locations_path)

# Select only relevant locations
locations_df = locations_df[locations_df.country__name == ingest_country_name]
locations_df = locations_df[locations_df.datetime_last__utc >= ingest_from_datetime_iso]
locations_df = locations_df[locations_df.datetime_first__utc <= ingest_to_datetime_iso]

loc_ids = locations_df.id.to_list()
print(f'Number of locs: {len(loc_ids)}')

import shutil
def clean_dir(dir):
    shutil.rmtree(dir)
    os.mkdir(dir) 

if pathlib.Path('./tmp').exists():
    clean_dir('./tmp')
else:
    os.mkdir('./tmp')

# Accumulate data locally
location_idx = 0
for loc_id in loc_ids:
    print(f'Loading measurements for loc {location_idx}/{len(loc_ids)}')

    for year in range(int(ingest_from_year), int(ingest_to_year) + 1):
        try:
            print('year: ', year)
            s3_src_dir = f's3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={year}/'
            tmp_dst_dir = f'./tmp/{year}/'
            correct = subprocess.run(['aws', 's3', 'cp', s3_src_dir, tmp_dst_dir, '--recursive'], check=True, text=True)#, shell=True ) # shell=True is for windows ONLY
        except Exception as e:
            print(f'Exception occured: {e=}. Moving to the next year...')
           
    location_idx += 1

# Eliminating that microfiles nightmare
# by packaging all locs and days data together
# into per month files.
# Then send it to the lake!
from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.bucket(gs_data_bucket[5:len(gs_data_bucket)-1]) # TODO: be careful
print("Starting the packing stage...")
for year in range(int(ingest_from_year), int(ingest_to_year) + 1):
    year_dir = f'./tmp/{year}/'
    subdirs = [x[0] for x in os.walk(year_dir)]
    subdirs = subdirs[1:]
    print(subdirs)
    for subdir in subdirs:
        mo = subdir[-2:]

        partial_dfs = []
        for p in pathlib.Path(subdir+'/').glob("*.csv.gz"):
            partial_dfs.append(pd.read_csv(p))

        mo_df = pd.concat(partial_dfs)
        mo_df['datetime'] = pd.to_datetime(mo_df['datetime'], utc=True)
        mo_df['datetime'] = pd.to_datetime(mo_df["datetime"]).apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))

        mo_df.to_parquet(f'./tmp/{mo}.parquet') 

        gcs_path = f'aq/raw/measurements/{ingest_country_name.lower()}/{year}/{mo}.parquet'
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(f'./tmp/{mo}.parquet')
        print(f'Packed and sent: {year}/{mo}.parquet')
