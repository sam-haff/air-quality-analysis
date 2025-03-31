import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb

@dlt.resource(name="openaq_locs")
def openaq_locs():
    client = RESTClient(
        base_url="https://api.openaq.org/v3/sensors/10419359/measurements",
        headers={"X-API-Key":"932148dc9fced6a1df5c6d006c2ab3ae249eb6076ad539c693487236ace264dc"},
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate(params={"limit":"10"}):
        for v in page:
            v["lat"] = 12
            v["lng"] = 24
        print(page)
        yield page
        break

pipeline_name = "open_aq_locs_load"
dataset_name = "open_aq"
table_name = "locs"

pipeline = dlt.pipeline(destination="filesystem", pipeline_name=pipeline_name, dataset_name=dataset_name)
load_info = pipeline.run(openaq_locs, table_name=table_name, loader_file_format="parquet", write_disposition="replace")
