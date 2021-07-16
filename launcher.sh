#!/bin/bash

#variables
REGION="us-central1"
ZONE="us-central1-a"
PROJECT="spry-reference-318517"
BUCKET_NAME="blankspace_bucket"
CLUSTER="pyspark-job"
LOCAL_DATASOURCE="/home/pratama/project/week3/"


#Copy file to GCS
# gsutil -m cp -r ${LOCAL_DATASOURCE} gs://${BUCKET_NAME}

# Create Dataproc Cluster
gcloud dataproc clusters create ${CLUSTER} \
    --project=${PROJECT} \
    --region=${REGION} \
    --zone=${ZONE} \
    --single-node

# Submit Spark Job 
gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} gs://storage_wk3/folder/app/pyspark_job.py --region=${REGION} --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar


# Delete Dataproc Cluster
gcloud dataproc clusters delete ${CLUSTER} --region=${REGION}



