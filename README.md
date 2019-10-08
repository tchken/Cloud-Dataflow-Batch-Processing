# Cloud-Dataflow-Batch-Processing

1. store dataset (.csv) in a Google Cloud Storage bucket.
2. create a Dataflow batch job that read and process the csv file.
3. in the Dataflow job, apply a "Group By" transform to get the count of listings by the "neighbourhood" field.
4. store both the original csv data and the transformed data into their own separate BigQuery tables.

<img src="https://github.com/TsungChinHanKen/Cloud-Dataflow-Batch-Processing/blob/master/resources/diagram.png" alt="alt text" width="500" height="500">

### gcloud command - project setting and check

- gcloud auth list
- gcloud config list project
- export PROJECT="<project-id>"
- gcloud config set project $PROJECT
- gsutil mb - c regional -l us-east4 gs://$PROJECT
- gsutil cp ./<data file path> gs://$PROJECT/
- bq mk <tablename>

- export GOOGLE_APPLICATION_CREDENTIALS="<filepath>/filename.json"

- bq show -j --project_id=<project_id dataflow_job>

### Local setup
- python2.7 -m virtual env
- source env/bin/activate
- deactivate (after job)

- pip freeze -r requirements.txt (enviroment setup)


### Execute pipeline (Local DirectRunner)
- python local_directrunner_pipeline.py

### Execute pipeline (DataFlow)
- python dataflow_pipeline.py\
--project=$PROJECT\
--runner=DataflowRunner\
--staging_location=gs://$PROJECT/temp\
--temp_location gs://$PROJECT/temp\
--input gs://$PROJECT/AB_NYC_2019.csv --save_main_session









