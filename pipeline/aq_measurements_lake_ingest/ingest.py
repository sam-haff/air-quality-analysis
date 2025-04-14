"""
Ingest data on micro timeframes. Ingest N last hours.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.helpers import requests
import pandas as pd

import os
from datetime import datetime, timedelta

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

gs_data_bucket = str(os.environ["AQ_DATA_BUCKET_URL"])
ingest_country_names = str(os.environ["AQ_COUNTRY_NAMES"])
ingest_from_datetime = str(os.environ["AQ_FROM_DATETIME_UTC"])
ingest_to_datetime = str(os.environ["AQ_TO_DATETIME_UTC"])
api_limit = int(os.environ["AQ_API_LIMIT"]) # should be very small as it refers to records per sensor and we that script is meant to be for narrow timeframes

print('Scripts run ingesting the data from: ', ingest_from_datetime)
print('Scripts run ingesting the data to: ', ingest_to_datetime)

# Keeping this in for local testing for now
#gs_data_bucket = 'gs://kestra-de-main-bucket/'
#ingest_country_names = 'Slovakia'
#ingest_from_datetime = '2025-04-07T00:00:01'
#ingest_to_datetime = '2025-04-07T08:00:00'

#api_limit = 20 #str(os.environ["AQ_API_LIMIT"]) # should be very small
#os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = C:/aq_data/check/"

dlt_download_path = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] 

print(f'Preparing to load realtime data for the following countries: {ingest_country_names}')

country_names = [nm.strip(' ') for nm in ingest_country_names.split(',')]

gs_raw_data_path =  '/aq/raw/'
gs_raw_data_path_url = urljoin(gs_data_bucket, gs_raw_data_path)
gs_prod_data_path_url = urljoin(gs_data_bucket, '/aq/data/') 

gs_topology_path = urljoin(gs_prod_data_path_url, "/sensors_topology/topology.parquet", ispath=False)
sensors_df = pd.read_parquet(gs_topology_path)

@dlt.resource(name="openaq_measurements")
def openaq_measurements():
    sensors_processed = 0
    total_loaded = 0
    for country in country_names:
        print('from: ', ingest_from_datetime)
        print('to: ', ingest_to_datetime)

        country_sensors = sensors_df[sensors_df.country__name== country]
        print('Country sensors: ')
        print(country_sensors)
        country_sensors = country_sensors[country_sensors.datetime_last__utc >= ingest_from_datetime]   # if not ingest_from > last and not ingest_to < first. ingest_from > last or ingest_to < from
        print('By from, filter:')
        print(country_sensors)
        country_sensors = country_sensors[country_sensors.datetime_first__utc <= ingest_to_datetime]
        print('By to, filter:')
        print(country_sensors)
        
        sensors = list(country_sensors.itertuples())
        for t in sensors:
            current_sensor = t.id_sensor

            client = RESTClient(
                base_url=f"https://api.openaq.org/v3/sensors/{current_sensor}/measurements",
                headers={"X-API-Key":"932148dc9fced6a1df5c6d006c2ab3ae249eb6076ad539c693487236ace264dc"},
                paginator=PageNumberPaginator(
                    base_page=1,
                    total_path=None
                )
            )

            print(f"Loading data for sensor id: {current_sensor}/{country_sensors.shape[0]}")
            try:
                for page in client.paginate(params={"limit":api_limit, "datetime_from":ingest_from_datetime, "datetime_to":ingest_to_datetime}):
                    for v in page:
                        print('Got record', v)
                        v['lat'] = t.coordinates__latitude
                        v['lon'] = t.coordinates__longitude
                        v['sensors_id'] = t.id_sensor
                        v['location_id'] = t.id_loc
                        v['location'] = t.name_loc
                    yield page
                    total_loaded += api_limit
                    print(f"Downloaded ~{api_limit} measurements. Total loaded: ", total_loaded)
            except requests.HTTPError as e:
                if (e.response.status_code == 429):
                    sensors.append(t)
                print('Got "Too many requests". Resoruce is back to queue to be processed later...')
            except Exception as e:
                print(f"Exception: {e=}. Moving to the next sensor...")

            sensors_processed += 1
            print("Finished loading a sensor. Sensors loaded: ", sensors_processed)

#from_part = 'bom'#datetime.datetime.fromisoformat(ingest_from_datetime).strftime("%Y_%m_%d_%H_%M_%S")
#to_part = 'pom'#datetime.datetime.fromisoformat(ingest_to_datetime).strftime("%Y_%m_%d_%H_%M_%S")
pipeline_name = "open_aq_measurements_load"
dataset_name = 'realtime'#f'{from_part}__{to_part}' 
table_name = "measurements"

pipeline = dlt.pipeline(destination="filesystem", pipeline_name=pipeline_name, dataset_name=dataset_name)
load_info = pipeline.run(openaq_measurements, table_name=table_name, loader_file_format="parquet", write_disposition="replace")

print("Download finished.")

print("Start processing...")


#loaded_data_path = urljoin(gs_raw_data_path_url, f'/{dataset_name}/', f'/{table_name}/')
#df = pd.read_parquet(loaded_data_path + '*')

#local_data_path = "C:/aq_data/check/realtime/measurements/"
#df = pd.read_parquet(local_data_path)

raw_data_path = urljoin(dlt_download_path, 'realtime/measurements/')
df = pd.read_parquet(raw_data_path)
df = df[["location_id", "sensors_id", "location", "period__datetime_from__utc", "lat", "lon", "parameter__name", "parameter__units", "value"]]

df = df.rename(columns={'period__datetime_from__utc':'datetime', 'parameter__name':'parameter', 'parameter__units': 'units'})

gs_realtime_data_path = urljoin(gs_prod_data_path_url, '/realtime_measurements/', 'realtime.parquet', ispath=False)#urljoin(gs_data_bucket, '/aq/data/') 
df.to_parquet(gs_realtime_data_path)
#df = df.rename(columns={'period__datetime_from__utc':'datetime', 'parameter__name':'parameter', 'parameter__units': 'units'})
#print(df)

