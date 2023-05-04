# DE Zoomcamp final project

## Abstract
Well, I was just vacationing in Turkey when I started the project, so I decided to research the weather in several Turkish cities for the last year.\
My pipeline get the data about weather in 3 Turkish cities: Ankara (the capital), Istanbul (the biggest city) and Antalya (the main resort) and transforms it to show two charts: average temperature by month and cities and weather conditions distribution. Retrieving data is done on monthly basis. <i> It'd be more native to use the daily basis for weather, but it'd exceed the free quota of API.</i>\
<i>Another reason to choose such topic was that it's rather difficult to find any datasets for batch processing. They should be stored with some periodicity, like monthly in our NY taxi example, and I failed to find any. So I decided to use requests strategy to some API to produce monthly data. Then I chose an API free of charge.</i>

## Technologies
- Cloud: GCP
- Infrastructure as code: Terraform
- Containerization: Docker
- Workflow orchestration: Airflow
- Data Wareshouse: BigQuery
- Batch processing: Spark
- Visualization: Google data studio

## Prior requirements
- You should have Google account
<br>and following components installed on your machine:
- Python via Anaconda
- Docker
- Terraform
<br>Hope you already have all of them thanks to DE zoomcamp course

## Result
Dashboard link:
https://lookerstudio.google.com/s/oGbi47yMWBo


## Instruction

### Google Cloud Platform

Probably you have already done many steps below, but let it be the full path just in case. 

#### Creating a project
Sign in in your Google account\
Go to https://console.cloud.google.com \
Select a project -> New project\
Project name “de-weather-project” -> create\
<i>Of course you can input another name</i>\
If you haven’n done it yet – activate your Free Trial at the top screen panel and then enter all required information.

#### Project administration: Cloud Console
IAM & Admin -> Service accounts -> Create service account\
Service account ID for example: “de-weather-project-user” -> create and continue -> select a role “Viewer” -> continue -> done\
On the next screen click on the service account name and then: Keys -> Add key -> Create new key -> json -> create -> close. Check where it is saved, usually it is your default Downloads directory.\
IAM & Admin -> IAM -> Edit principal with “Viewer” role\
Add another role (several): Storage Admin + Storage Object Admin + BigQuery Admin + Dataproc Administrator + Service Account User. Pay attention to the last two: they are necessary for making data transformations on Google Hadoop cluster.\
Enable APIs (may be already enabled if it is not your first project on GCP):\
•	https://console.cloud.google.com/apis/library/iam.googleapis.com \
•	https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com \
•	https://console.cloud.google.com/apis/library/dataproc.googleapis.com \
If it is your first project, sometimes GCP can’t connect it to the billing account. Do this on your own if you get an appropriate warning. Don’t worry, you have the Free Trial period.\
Install SDK following this instruction:\
https://cloud.google.com/sdk/docs/install-sdk

#### Project administration: next steps in terminal

Put the .json key (downloaded before) into .gc folder (create if it doesn’t exist) and set the env var:
```
mkdir .gc
mv <downloads-folder-or-where-the-key-was-saved>.json ${HOME}/.gc/google_credentials.json
export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/.gc/google_credentials.json
```
You may want to add this export to .bashrc file if you use a Linux-based OS and don’t want to re-execute it every time you log-in your machine. 

Now verify authentication:
```
gcloud auth application-default login
```
or, if you use a machine without browser (Google VM for example), run this alternative:
```
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
```

### API
To get the data you have to subscript on RapidApi – the service which provides a large set of different APIs.\
https://rapidapi.com/ \
Sign up – you’ll be asked to do several typical steps, no need to use a credit card.\
Then find “Visual Crossing Weather” API and subscribe to the Basic free plan.\
Go to the API documentation and find your api-key there in Header Parameters.

### Next

Get this repo (do git clone or do whatever you want)

Next steps here:
<a href="https://github.com/avyazg/de-zoomcamp-project/blob/main/terraform/readme.md">terraform</a>
