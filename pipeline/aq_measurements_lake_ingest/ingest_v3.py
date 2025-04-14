'''
Ingests data from openaq given the datetime range.
Uses boto3 to download data from openaq s3 bucket.
Isn't feasible to use because:
a) It's slower than using Amazon CLI(ingest_v2.py).
b) S3 data is outdated by weeks.
'''

import subprocess
import pandas as pd
import os
import pathlib
import numpy as np
import boto3

def download_dir(client, resource, dist, local='/tmp', bucket='your_bucket'):
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
        if result.get('CommonPrefixes') is not None:
            for subdir in result.get('CommonPrefixes'):
                download_dir(client, resource, subdir.get('Prefix'), local, bucket)
        for file in result.get('Contents', []):
            dest_pathname = os.path.join(local, file.get('Key'))
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            if not file.get('Key').endswith('/'):
                resource.meta.client.download_file(bucket, file.get('Key'), dest_pathname)
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


#gs_data_bucket = os.environ["AQ_DATA_BUCKET_URL"] 
#ingest_from_year = os.environ["AQ_FROM_YEAR"]
#ingest_from_month= os.environ["AQ_FROM_MO"]
#ingest_to_year = os.environ["AQ_TO_YEAR"]
#ingest_to_month = os.environ["AQ_TO_MO"]
#ingest_country_name = os.environ["AQ_COUNTRY_NAME"]

gs_data_bucket = 'gs://kestra-de-main-bucket/'
ingest_year = '2025'
ingest_month= '04'
ingest_day = '05'
#ingest_to_year = '2025'
#ingest_to_month = '03'
ingest_country_name = 'Slovakia'

ingest_from_datetime_iso = f'{ingest_year}-{ingest_month}-{ingest_day} 00:00:00'
ingest_to_datetime_iso = f'{ingest_year}-{ingest_month}-{ingest_day} 23:59:59'

gs_raw_data_path =  '/aq/raw/'
gs_raw_data_path_url = urljoin(gs_data_bucket, gs_raw_data_path)
gs_prod_data_path_url = urljoin(gs_data_bucket, '/aq/data/')

print('URL: ', gs_raw_data_path_url)
gs_locations_path = urljoin(gs_raw_data_path_url, '/sensors_topology/locs/')
locations_df = pd.read_parquet(gs_locations_path)

locations_df = locations_df[locations_df.country__name == ingest_country_name]
locations_df = locations_df[locations_df.datetime_last__utc >= ingest_from_datetime_iso]   # if not ingest_from > last and not ingest_to < first
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

cl = boto3.client('s3')
# accumulate data locally
location_idx = 0
for loc_id in loc_ids:
    print(f'Loading measurements for loc {location_idx}/{len(loc_ids)}')

    try:
        print('year: ', ingest_year)
        s3_src_dir = f's3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={ingest_year}/month={ingest_month}/location-{loc_id}-{ingest_year}{ingest_month}{ingest_day}.csv.gz'
        print('aws link: ', s3_src_dir)
        tmp_dst_dir = f'./tmp/'
        cl.download_file('openaq-data-archive',  f'records/csv.gz/locationid={loc_id}/year={ingest_year}/month={ingest_month}/location-{loc_id}-{ingest_year}{ingest_month}{ingest_day}.csv.gz', f'./tmp/location-{loc_id}-{ingest_year}{ingest_month}{ingest_day}.csv.gz')
        #correct = subprocess.run(['aws', 's3', 'cp', s3_src_dir, tmp_dst_dir], check=True, text=True, shell=True ) # shell=True is for windows ONLY
    except Exception as e:
        print(f'Exception occured: {e=}. Moving to the next year...')
    #bound_mo = 12
    #if year == ingest_to_year:
    #    bound_mo = ingest_to_month 
    
    location_idx += 1

# Eliminating that microfiles nightmare
# by packaging all locs and days data together
# into per month files.
# Then send it to the lake!
exit(1)
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
