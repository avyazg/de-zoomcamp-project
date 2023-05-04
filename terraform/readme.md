Go to terraform folder and execute this:
```
cd <folder where you store this project>/de-zoomcamp-project/terraform
terraform init
terraform plan
-	Enter your GCP project ID (“de-weather-project” if you follow this manual word by word)
terraform apply
-	Enter your GCP project ID
```

<i>You may fail creating a Dataproc cluster via Terraform. So, I faced some issues testing the project on my personal Mac which I did not developing the project on GCP VM. I renewed the code considering solution of those problems, but maybe you’ll face some other. 
If you fail using Terraform, complete these actions:
- On Cloud Console, go to Dataproc -> create cluster -> on Compute Engine -> create,
- Input name “de-project-cluster”, region “europe-west1”, Zone doesn’t matter
- Configure nodes -> Manager node: series: N1; Worker nodes: series: N1
- Click Create
- Enable Cloud Dataproc API (and maybe other APIs), add role Service Account User
- Go to Compute Engine, and give permissions to each of your VMs (master, workers - for all of them):
    - Click on machine’s name -> Edit -> Access scopes: Allow full access to all cloud APIs -> Save
</i>

Next steps here:
<a href="https://github.com/avyazg/de-zoomcamp-project/blob/main/airflow/readme.md">airflow</a>
