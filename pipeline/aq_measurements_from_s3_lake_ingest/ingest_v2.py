import subprocess
import pandas as pd
import os
import datetime
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


#gs_data_bucket = os.environ["AQ_DATA_BUCKET_URL"] 
#ingest_from_year = os.environ["AQ_FROM_YEAR"]
#ingest_from_month= os.environ["AQ_FROM_MO"]
#ingest_to_year = os.environ["AQ_TO_YEAR"]
#ingest_to_month = os.environ["AQ_TO_MO"]
#ingest_country_name = os.environ["AQ_COUNTRY_NAME"]

gs_data_bucket = 'gs://kestra-de-main-bucket/'
ingest_from_year = '2024'
ingest_from_month= '01'
ingest_to_year = '2025'
ingest_to_month = '03'
ingest_country_name = 'Slovakia'

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

locations_df = locations_df[locations_df.country__name == ingest_country_name]
locations_df = locations_df[locations_df.datetime_last__utc >= ingest_from_datetime_iso]   # if not ingest_from > last and not ingest_to < first
locations_df = locations_df[locations_df.datetime_first__utc <= ingest_to_datetime_iso]

loc_ids = locations_df.id.to_list()
print(f'Number of locs: {len(loc_ids)}')

import shutil
def clean_dir(dir):
    shutil.rmtree(dir)
    os.mkdir(dir) 
    #for i in os.listdir(dir):
    #    os.rmdir(i)

#aws s3 cp
location_idx = 0
for loc_id in loc_ids:
    print(f'Loading measurements for loc {location_idx}/{len(loc_ids)}')

    for year in range(int(ingest_from_year), int(ingest_to_year) + 1):
        try:
            print('year: ', year)
            if pathlib.Path('./tmp').exists():
                clean_dir('./tmp')

            s3_src_dir = f's3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={year}/'
            tmp_dst_dir = './tmp/'
            #correct = subprocess.run(['gsutil', '-m', 'cp', '-R', s3_src_dir, gcs_dst_dir], check=True, text=True ) # shell=True is for windows ONLY
            correct = subprocess.run(['aws', 's3', 'cp', s3_src_dir, tmp_dst_dir, '--recursive'], check=True, text=True, shell=True ) # shell=True is for windows ONLY
            #df = pd.read_csv(tmp_dst_dir)
            #df.to_parquet(gcs_dst_dir)

            partial_dfs = []
            print('Printing magical things...')
            for p in pathlib.Path(tmp_dst_dir).glob("**/*.csv.gz"):
                print(p)
                partial_dfs.append(pd.read_csv(p))
            print(len(partial_dfs))
            df = pd.concat(partial_dfs)

            gcs_dst_dir = urljoin(gs_data_bucket, f'aq/raw/measurements/{ingest_country_name.lower()}/{year}/{loc_id}.parquet')
            df.to_parquet(gcs_dst_dir)
            #subprocess.run(['rm', f'{tmp_dst_dir}*']) #linux
#del /S C:\Path\to\directory\*
            #subprocess.run(['del', '/S', f'{tmp_dst_dir}*'])
        except Exception as e:
            print(f'Exception occured: {e=}. Moving to the next year...')
            #exit(1)
        location_idx += 1
        bound_mo = 12
        if year == ingest_to_year:
            bound_mo = ingest_to_month 
        #for month in range(int(ingest_from_month), int(ingest_to_month)+1):
        #    s3_src_dir = f's3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={year}/month={month:02d}/'
        #    gcs_dst_dir = urljoin(gs_data_bucket, f'aq/raw/measurements/{ingest_country_name.lower()}/{year}/')
        #    print('copying to ', gcs_dst_dir)
            #gsutil -m cp -R s3://openaq-data-archive/records gs://kestra-de-main-bucket/openaq-data/
            #gcloud storage cp -m s3://openaq-data-archive/records/ gs://kestra-de-main-bucket/openaq-data/ --recursive

        #    correct = subprocess.run(['gsutil', '-m', 'cp', '-R', s3_src_dir, gcs_dst_dir], check=True, text=True, shell=True) # shell=True is for windows ONLY
            #correct = subprocess.run(['g', 'storage', 'cp', s3_src_dir, gcs_dst_dir, '--recursive'], check=True, text=True, shell=True) # shell=True is for windows ONLY
