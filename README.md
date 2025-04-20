### AIRQ ###
Air quality dashboard and analysis project.
### Motivation ###
Having a data platform that provides readily available data in a format suitable for processing is essential for extracting insights across all data-related fields.\
Air pollution is a pressing issue today, and to properly address it, we first need access to reliable data. Without data, it's as if the problem doesn't exist — data serves as our eyes, enabling us to perceive and understand the issue.\
However, simply having data isn't enough. It must remain relevant, be consistently updated, and be stored in formats that allow for efficient processing and analysis.\
This project aims to solve those challenges for air quality data — ensuring accessibility, relevance, and usability — while also developing analytics that clearly communicate the current state of air pollution.
### Report ###
Report generated with ingestred data for Slovakia, Hungary and Poland. [Looker](https://lookerstudio.google.com/reporting/6e8121ae-214b-4610-9237-506421e5d3c8)

<img src="https://github.com/user-attachments/assets/71e07766-fc3a-472b-90ba-332732c29e35" width=600 height=400>

### Cloud infrastructure diagram ###

<img src="https://github.com/user-attachments/assets/c23b9b15-7e28-4ae5-8037-8e0bcb24e00d" width=500 height=400>

### Getting started ###
#### Access ####
Project assumes you have a service account with the following roles/rights on it:
- Storage Admin
- Compute Admin
- BigQuery Admin
- SQL Admin
- Cloud SQL Admin
- Compute Network Admin
- Network Management Admin
- Networks Admin
- Service Networking Admin
#### Get the necessary cloud infrastrcucture up and running ####
Since the project uses Terraform, that's quite simple to do.
You should just change the default values in the file <em>./terraform/variables.tf</em> to the values that make sense for you.
Then, given you have <em>Terraform</em> installed, you run 
```
terraform init
terrafrom apply
```
#### SQL ####
Find your managed SQL instance in your GCP console. You should add seperate database and also register new user.
#### Run kestra on the VM ####
Now that you have the cloud infrastructure up, you also have VM and managed SQL instances.

You should ssh into your VM and do download the Kestra docker compose file:
~~~
curl -o docker-compose.yml \
https://raw.githubusercontent.com/kestra-io/kestra/develop/docker-compose.yml
~~~
Remove database service from it. Now, you should have only kestra service description in it. In the description there is a large environment variable defined called KESTRA_CONFIGURATION. It contains "datasources" element, which in turn contains database credentials. You should change them to correspond to your managed SQL instance.
Also, for security purposes, it is recommended to setup basicAuth(in same env var) to be enabled(enable: true) and customize the credentials(username and password).
That's it, now you just save the file and run:
~~~
sudo docker compose up
~~~
#### Installing the flows ####
Flows are submitted to the Kestra instance via Github Actions. So you just commit anything, and that will automatically transfer all the flows to your Kestra instance.
For that to work, you need to setup secrets in your Github repo.
- KESTRA_INST_ADDR - the address of your VM with the port of your Kestra service(default: 8080).
- KESTRA_USER: username from credentials(basicAuth) you had set in the kestra configuration.
- KESTRA_PWD: password from credentials(basicAuth) you had set in the kestra configuration.

Other options:
- use ./utils/deploy_flow util.
- manually create flows one by one by creating them from UI and copying the content from the repository.
#### Setting up kestra environment ####
Before flows could be run, you need to set the following KVs in the company.team namespace.

- AQ_API_KEY - OpenAQ API key(requires registration on their platform)
- AQ_DATA_BUCKET_URL - url of the bucket in the following form: "gs://bucket-name/"
- GCP_CREDS - Google Cloud Platform service account json
- GCP_PROJECT - Google Cloud project
- GCP_LOC - Google Cloud project location(example: "US")
- GCP_BUCKET - Google Cloud Storage bucket(required default bucket)
- DBT_ACCOUNT_ID - (optional) DBT account id
- DBT_BUILD_JOB_ID - (optional) Id of DBT job to run
- DBT_BASE_URL - (optional) DBT base url(url of your dbt intance)
- DBT_API_TOKEN - (optional) DBT api token
- DBT_PROFILES - (optional) DBT profiles.yml file content


#### DBT ####
You can build analyics with executing 'dbt_local_run_build' flow. \
To be able to use this flow, you will need the correct 'profiles.yaml' file, which contains credentials for the BigQuery connection. Given you already have the file, you now need to copy its contents to your new Kestra KV 'DBT_PROFILES'.
Indirectly, it's called from realtime ingestion flow, but it should be explicitly enabled and you still need to set up all the required KVs(see comments in .yaml).\
And, of course, you can build the project as usual, both on the local machine and CloudDBT.
### Pipeline ###
#### Diagrams ####
Flow **ingest_air_quality_measurements_from_openaq_s3**

<img src="https://github.com/user-attachments/assets/92b7e215-d23c-4ed0-9a05-40fbaeb32660" width=550 height=220>

Flow **ingest_air_quality_measurements_from_api_microtimeframes**

<img src="https://github.com/user-attachments/assets/331b01cd-7633-45df-acfe-44373309f9c6" width=700 height=220>

#### Overview ####
Project relies on the OpenAQ service to collect the date about air quality. Even though OpenAQ is a great platform, the process of collecting data from it turned out to be surprisingly challenging.
There are two ways of retrieving the data from the service:
1) API 
2) S3 bucket

API itself is a very convenient way of extracting the data, at least it seemed so on the paper. In reality, for unpaid accounts, it has pretty agressive rate limits. And with certain access pattern that still results in acceptable bandwidth. But if you need 1 record from each location and sensor and API gives you 60 records per minute max, then you are just getting these 60 records per minute. Even with good pattern not everything is transparent, sometime the service throws a 503 error after 15k records telling that the requested workload for the request is too high. But it wasn't too high for previous 15 1k records requests. And also changing the page limit to 300 doesn't change anything, it still gives status code 500 after the exact same 15k records.
So, I thought S3 might be a solution since it's stated that it's updated on hourly basis. But 
- It isn't(or updates come in some strange specific pattern, may be it prioritises some sensors/locations over the others)
- It contains millions of files, and it makes the download speed not that good, as I was initially anticipating.

Given these specifics, it was decided to do the following:
- S3 bucket is used for initial ingestion(i.e several years of history data).
- Realtime data is ingested via API(slow but it's the only source of up to date data). The corresponding flow is scheduled to be run every 8h.

Before any ingestion can happen, first we need the description of the sensors topology. It contains the information about all available locations across the full histroy timeline. It includes information about the country, concrete geo(latitude, longitude) and time range, on which the location was active. All that is useful for the ingestion operations to ingest data selectively(base on the country and/or time range).
