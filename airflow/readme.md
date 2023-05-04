Start Docker. Usually, it means to run Docker Desktop application, but you can do this how you wish to.

Go to airflow folder:
```
cd <folder where you store this project>/de-zoomcamp-project/airflow
```
Modify .env file. If you follow this instruction word by word, you should make only one change:
```
RAPIDAPI_KEY=<got on the previous step>
```
Actually, there were only changes you should make in the code! If you use your desired names for some services, adjust them as well.

Now you should transfer the data transformation pyspark code from local to GCP: 
```
gsutil cp <folder where you store this project>/de-zoomcamp-project/spark/data_transform_spark.py gs://data_lake_de-weather-project/code/data_transform_spark.py
```
If you use another bucket name, change it. 

Run the following:
```
docker-compose build
docker-compose up
```

If you use some VM, forward port 8080 in order to view Airflow UI in the browser on your local machine.

Go to http://localhost:8080 in your browser, credentials are: user – airflow, pass – airflow.

Manually trigger 2 dags in the same order (top down). For clarity you can enter them and observe the process.\
For each of 2 dags: click the name of dag, then click "trigger" (play button). And wait until it finishes 12 months (until green steps stop appearing).
 
<i>In fact, right now all historical runs of the second dag produce the same result, because we triggered it after all scheduled runs of the first dag have been executed, and the second one takes the same external table each time. But the idea is that in production mode the second dag starts working in 3 hours after the first one (see the scheduled time: 06:00:00 and 09:00:00), so it will pick up the fresh result of the first one. For the training course it would be enough to set the schedule interval for the second dag into “@once” mode, but for better simulation of production mode I set it as it should be. 

Again, you can face some problems on the “run_pyspark_script” task. In my case they were associated with permissions, and on different machines they were different. I tried to consider everything in my code, but if you fail, you can try following (separate steps out of order):  
-	Check that your service account has the Service Account User role
-	On GCP go to Compute Engine, and give permissions to each of your VMs (master, workers, regular VM (if you run the code there) – for all of them):
    -	Click on machine’s name -> Edit -> Access scopes: Allow full access to all cloud APIs -> Save
-	In Terminal:
```
docker ps
```
  
for each container of airflow bundle:
```
docker exec -it <container id> bash
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
export CLOUDSDK_CORE_PROJECT=$GCP_PROJECT_ID
exit
```
- Just run this directly on your machine, ignoring docker:
```
gcloud dataproc jobs submit pyspark \
                --cluster=de-project-cluster \
                --region=europe-west1 \
                --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
                gs://data_lake_de-weather-project/code/data_transform_spark.py \
                -- \
                --dataset=de_project_dataset
```
</i>
