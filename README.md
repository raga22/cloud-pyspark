# cloud-pyspark

### program flow

### how to run
1. create storage in GCS, upload the dataset (json) and spark job (pyspark_job.py)
2. create bigquery dataset and create table flight and total_flight_distance (the schema you can see on flight_schema.json and total_flight_schema.json)
3. edit the spark job and launcher.sh file, customize with your bucket name and bigquery dataset
```
BUCKET = "blankspace_bucket"
PROJECT = "spry-reference-318517"
DATASET = "blankspace"
```
5. run lancher.sh in you terminal (linux), command : ./launcher.sh
