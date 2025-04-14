'''
Ingests data from openaq given the datetime range.
Uses Amazon CLI to download data from openaq s3 bucket.
It is the most efficient way to download the data, 
given that datetime range is day-by-day granularity
(meaning that you can load half a month with it).

Because there are a lot files("number of days" times "number of locations" 
times "months" times "years"), uploading them on file by file basis
is very slow. So instead, we first download them to the machine.
After we have files locally - unite the data, so that output is a parquet file per year-month.
Then we send these files to the target bucket.
'''

import subprocess
import pandas as pd
import os
import pathlib

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

# untested
def download_range(loc_id, from_year, from_month, from_day, to_year, to_month, to_day):
    location_idx = 0
    to_day_i = 31 # if no data just move on
    from_day_i = int(from_day)
    from_mo_i = int(from_month)
    to_mo_i = int(to_month)
    for loc_id in loc_ids:
        for year in range(int(from_year), int(to_year) + 1):

            from_mo_i = 1
            to_mo_i = 12
            if (year == int(from_year)):
                from_mo_i = int(from_month)
            elif (year == int(to_year)):
                to_mo_i = int(to_month)
            
            for mo in range(from_mo_i, to_mo_i+1):
                from_day_i = 1
                to_day_i = 31
                if (year == int(from_year) and mo == int(from_month)):
                    from_day_i = int(from_day)
                elif (year == int(to_year) and mo == int(to_month)):
                    to_day_i = int(to_day)
                
                for day in range(from_day_i, to_day_i+1):
                    download_day(loc_id, year, f'{mo:02d}', f'{day:02d}')
                    

def download_day(loc_id, year, month, day):
    try:
        print('year: ', year)
        s3_src_dir = f's3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={year}/month={month}/location-{loc_id}-{year}{month}{day}.csv.gz'
        print('aws link: ', s3_src_dir)
        tmp_dst_dir = f'./tmp/'
        correct = subprocess.run(['aws', 's3', 'cp', s3_src_dir, tmp_dst_dir], check=True, text=True, shell=True ) # shell=True is for windows ONLY
    except Exception as e:
        print(f'Exception occured: {e=}. Moving to the next year...')


gs_data_bucket = os.environ["AQ_DATA_BUCKET_URL"] 
ingest_month= os.environ["AQ_MONTH"]
ingest_year = os.environ["AQ_YEAR"]
ingest_day = os.environ['AQ_DAY']
ingest_country_name = os.environ["AQ_COUNTRY_NAME"]

# For local testing
#gs_data_bucket = 'gs://kestra-de-main-bucket/'
#ingest_year = '2024'
#ingest_month= '01'
#ingest_day = '01'
#ingest_country_name = 'Slovakia'

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

# accumulate data locally
location_idx = 0
for loc_id in loc_ids:
    download_day(loc_id, ingest_year, ingest_month, ingest_day)

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
        # Changing datetime to smth BQ external tables can understand:w
        
        mo_df['datetime'] = pd.to_datetime(mo_df['datetime'], utc=True)
        mo_df['datetime'] = pd.to_datetime(mo_df["datetime"]).apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))

        mo_df.to_parquet(f'./tmp/{mo}.parquet') 

        gcs_path = f'aq/raw/measurements/{ingest_country_name.lower()}/{year}/{mo}.parquet'
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(f'./tmp/{mo}.parquet')
        print(f'Packed and sent: {year}/{mo}.parquet')
