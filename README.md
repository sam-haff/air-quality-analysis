### AIRQ ###
Air quality dashboard and analysis project.
### Report ###
Report generated with ingestred data for Slovakia, Hungary and Poland.

<img src="https://github.com/user-attachments/assets/71e07766-fc3a-472b-90ba-332732c29e35" width=600 height=400>

### Cloud infrastructure diagram ###

<img src="https://github.com/user-attachments/assets/c23b9b15-7e28-4ae5-8037-8e0bcb24e00d" width=500 height=400>

### Getting started ###
#### Get the necessary cloud infrastrcucture up and running ####
Since the project uses Terraform, that's quite simple to do.
You should just change the default values in the file <em>./terraform/variables.tf</em> to the values that make sense for you.
Then, given you have <em>Terraform</em> installed, you run 
```
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
Remove database service from it. Now you should have only kestra service description in it. In the description there is a large environment variable defined called KESTRA_CONFIGURATION. It contains "datasources" element, which in turn contains database credentials. You should change them to correspond to your managed SQL instance.
Also, for security purposes, it is recommended to setup basicAuth(in same env var) to be enabled(enable: true) and customize the credentials(username and password).
That's it, now you just save the file and run:
~~~
sudo docker compose up
~~~
#### Installing the flows ####
Flows are submitted to the Kestra instance via Github Actions. So you just commit anything and that will automatically transfer all the flows to your Kestra instance.
For that to work you need to setup secrets in your Github repo.
KESTRA_INST_ADDR - the address of your VM with the port of your Kestra service(default: 8080).
KESTRA_USER: username from credentials(basicAuth) you had set in the kestra configuration.
KESTRA_PWD: password from credentials(basicAuth) you had set in the kestra configuration.

Other options:
- use ./utils/deploy_flow util.
- manually create flows one by one by creating them from UI and copying the content from the repository.
#### Setting up kestra environment ####
Before flows could be run, you need to set the following KVs in the company.team namespace.

AQ_API_KEY - OpenAQ API key(requires registration on their platform)
AQ_DATA_BUCKET_URL - url of the bucket in the following form: "gs://bucket-name/"
GCP_CREDS - Google Cloud Platform service account json
GCP_PROJECT - Google Cloud project
GCP_LOC - Google Cloud project location(example: "US")
GCP_BUCKET - Google Cloud Storage bucket(required default bucket)
DBT_ACCOUNT_ID - (optional) DBT account id
DBT_BUILD_JOB_ID - (optional) Id of DBT job to run
DBT_BASE_URL - (optional) DBT base url(url of your dbt intance)
DBT_API_TOKEN - (optional) DBT api token


#### DBT ####
Create your DBT cloud project on the project repository and specify subpath as ./dbt. That's it.
To build analytics, run:
~~~
dbt build
~~~
### Pipeline ###
#### Diagrams ####
Flow **ingest_air_quality_measurements_from_openaq_s3**

<img src="https://github.com/user-attachments/assets/63bd5bfb-825b-463b-a2c5-08f49e598df9" width=520 height=220>

Flow **ingest_air_quality_measurements_from_api_microtimeframes**

<img src="https://github.com/user-attachments/assets/331b01cd-7633-45df-acfe-44373309f9c6" width=670 height=220>

#### Overview ####
Project relies on the OpenAQ service to collect the date about air quality. Even though OpenAQ is a great platform, the process of collecting data from it turned out to be surprisingly challenging.
There are two ways of retrieving the data from the service:
1) API
2) S3 bucket
API itself is a very convenient way of extracting the data, at least it seemed so on the paper. In reality, for unpaid accounts, it has pretty agressive rate limits. And with certain access pattern that really becomes a big deal. For example, giving one location and sensor you can extract 1000 records per cost of 1 in terms of rate limit(which is 60 requests per minute). But if you need 1 record from each location and sensor it gives you 60 records per minute max. Even with good pattern not everything is transparent, sometime the service throws a 503 error after 15k records telling that the requested workload for the request is too high. But it wasn't too high for previous 15 1k records requests. And also changing the page limit to 300 doesn't change anything, it still gives status code 500 after the exact same 15k records.
So, I thought S3 might a solution since it's stated that it's updated on hourly basis. But 
- It isn't(or updates come in some strange specific pattern, may be it prioritises some sensors/locations over the others)
- It contains millions of files, and it makes the download speed not that good, as I was initially anticipating.

Given these specifics, it was decided to do the following:
- S3 bucket is used for initial ingestion(i.e several years of history data).
- Realtime data is ingested via API(slow but it's the only source of up to date data). The corresponding flow is scheduled to be run every 8h.

Before any ingestion can happen, first we need the description of the sensors topology. It contains the information about all available locations across the full histroy timeline. It includes information about the country, concrete geo(latitude, longitude) and time range, on which the location was active. All that is useful for the ingestion operations to ingest data selectively(base on the country and/or time range).

### Motivation ###
Having a dataplatform that provides readily available data in the suitable for processing format is crucial for extracting insights in every data related field.
As air pollution problem is clearly very relevant nowadays, to able to acknowledge it we need the data. Otherwise, if we don't have the data it the same if we didn't see a problem. The data is our eyes in some sense.
Sadly, having the data is not enough, as it also needs to stay relevant and be stored in the right format for efficient updates and processing.
This projects aims to achieve that for the air quality data, while also developing the analytics to present the state of current air pollution as clear as possible.
