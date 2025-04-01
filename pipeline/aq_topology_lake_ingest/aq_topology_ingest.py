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


@dlt.resource(name="openaq_locs")
def openaq_locs():
    client = RESTClient(
        base_url="https://api.openaq.org/v3/locations",
        headers={"X-API-Key":"932148dc9fced6a1df5c6d006c2ab3ae249eb6076ad539c693487236ace264dc"},
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate(params={"limit":"1000"}):
        yield page
        print("Downloaded ~1000 locs.")


pipeline_name = "open_aq_locs_load"
dataset_name = "sensors_topology"
table_name = "locs"

pipeline = dlt.pipeline(destination="filesystem", pipeline_name=pipeline_name, dataset_name=dataset_name)
load_info = pipeline.run(openaq_locs, table_name=table_name, loader_file_format="parquet", write_disposition="replace")

print("Download finished.")

import pandas as pd

sensors_df = pd.read_parquet(urljoin(gs_raw_data_path_url, f"/{dataset_name}/locs__sensors/"))
locs_df = pd.read_parquet(urljoin(gs_raw_data_path_url, f"/{dataset_name}/locs/")) 

enr_df = sensors_df.set_index('_dlt_parent_id').join(locs_df.set_index('_dlt_id'), lsuffix="_sensor", rsuffix="_loc")

selected_cols = ['id_sensor', 'name_sensor', 'parameter__id', 'parameter__name', 'parameter__units', 'parameter__display_name', 'id_loc', 'name_loc', 'timezone', 'country__id', 'country__code', 'country__name', 'coordinates__latitude', 'coordinates__longitude', 'datetime_first__utc', 'datetime_last__utc']
sensors_final_df = enr_df[selected_cols]
gs_topology_output_path = urljoin(gs_prod_data_path_url, "/sensors_topology/topology.parquet", ispath=False)
sensors_final_df.to_parquet(gs_topology_output_path)

#romania_country_id = 74
#romania_sensors_df = sensors_final_df[sensors_final_df.country__id == romania_country_id]
#print(romania_sensors_df)
#for t in romania_sensors_df.itertuples():
#    print(t)
#    break
