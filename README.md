# Cloud-Dataflow-Batch-Processing

1. store dataset (.csv) in a Google Cloud Storage bucket.
2. create a Dataflow batch job that read and process the csv file.
3. in the Dataflow job, apply a "Group By" transform to get the count of listings by the "neighbourhood" field.
4. store both the original csv data and the transformed data into their own separate BigQuery tables.
