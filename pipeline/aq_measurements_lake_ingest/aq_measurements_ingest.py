import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import os

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


gs_data_bucket = os.environ["AQ_DATA_BUCKET_URL"] # TODO: change to DLT enr var so that we are not setting same value twice
if len(gs_data_bucket) == 0:
    print("No data bucket url is set")
    exit(1)

gs_raw_data_path =  '/aq/raw/'
gs_raw_data_path_url = urljoin(gs_data_bucket, gs_raw_data_path)
gs_prod_data_path_url = urljoin(gs_data_bucket, '/aq/data/')

import pandas as pd
import datetime

gs_topology_path = urljoin(gs_prod_data_path_url, "/sensors_topology/topology.parquet", ispath=False)
sensors_df = pd.read_parquet(gs_topology_path)
#sensors_df = sensors_df[sensors_df['country__name'].unique()]
#print(sensors_df)
#exit(0)
#sensors_final_df.to_parquet(gs_topology_output_path)

from_year = '2024'
from_month = '01'
from_day = '01'
from_hour = '00'
to_year = '2025'
to_month = '03'
to_day = '31'
to_hour = '00'

timeframe_from_str = f'{from_year}-{from_month}-{from_day} {from_hour}:00:00'
timeframe_from_dt = datetime.datetime.fromisoformat(timeframe_from_str)
timeframe_to_str = f'{to_year}-{to_month}-{to_day} {to_hour}:00:00'
timeframe_to_dt = datetime.datetime.fromisoformat(timeframe_from_str)
romania_country_id = 76#poland 77 # ro 74
romania_sensors_df = sensors_df[sensors_df.country__id == romania_country_id]
romania_sensors_df = romania_sensors_df[romania_sensors_df.datetime_last__utc > timeframe_from_str]
#romania_sensors_df = romania_sensors_df.sample(25)
print(romania_sensors_df)
exit(0)

@dlt.resource(name="openaq_measurements")
def openaq_measurements():
    sensors_processed = 0
    total_loaded = 0
    for t in romania_sensors_df.itertuples():
        current_sensor = t.id_sensor
        client = RESTClient(
            base_url=f"https://api.openaq.org/v3/sensors/{current_sensor}/measurements",
            headers={"X-API-Key":"932148dc9fced6a1df5c6d006c2ab3ae249eb6076ad539c693487236ace264dc"},
            paginator=PageNumberPaginator(
                base_page=1,
                total_path=None
            )
        )

        print(f"Loading data for sensor id: {current_sensor}")
        try:
            for page in client.paginate(params={"limit":"1000", "datetime_from":timeframe_from_str, "datetime_to":timeframe_to_str}):
                for v in page:
                    v['latitude'] = t.coordinates__latitude
                    v['longitude'] = t.coordinates__longitude
                yield page

                total_loaded += 1000 
                print("Downloaded ~1000 measurements. Total loaded: ", total_loaded)
        except Exception as e:
            print(f"Exception: {e=}. Moving to the next sensor...")

        sensors_processed += 1
        print("Finished loading a sensor. Sensors loaded: ", sensors_processed)

pipeline_name = "open_aq_measurements_load"
dataset_name = f"{from_year}-{from_month}-{from_day}-{from_hour}__{to_year}-{to_month}-{to_day}-{to_hour}"
table_name = "measurements"

pipeline = dlt.pipeline(destination="filesystem", pipeline_name=pipeline_name, dataset_name=dataset_name)
load_info = pipeline.run(openaq_measurements, table_name=table_name, loader_file_format="parquet", write_disposition="replace")

print("Download finished.")

