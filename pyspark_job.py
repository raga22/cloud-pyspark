from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType

BUCKET = "blankspace_bucket"
PROJECT = "spry-reference-318517"
DATASET = "blankspace"

spark = SparkSession \
    .builder \
    .appName("pyspark_job") \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
spark.conf.set('temporaryGcsBucket', BUCKET)

schema = StructType([
    StructField("flight_date",DateType()),
    StructField("airline_code",StringType()),
    StructField("flight_num", IntegerType()),
    StructField("source_airport", StringType()),
    StructField("destination_airport",StringType()),
    StructField("departure_time", IntegerType()),
    StructField("departure_delay", IntegerType()),
    StructField("arrival_time", IntegerType()),
    StructField("arrival_delay", IntegerType()),
    StructField("airtime", IntegerType()),
    StructField("distance", LongType()),
    StructField("id", IntegerType()),
])

#read data json from local storage
# flight = spark.read.schema(schema) \
#     .json("/home/pratama/project/week3/Dataset/*.json")


#read data json from google cloud storage
flight = spark.read.schema(schema) \
    .json(f"gs://{BUCKET}/week3/Dataset/*.json")


#create temp table, for access by query
flight.createOrReplaceTempView('flight')

total_flight_distance = spark.sql(
    "SELECT flight_date, airline_code, sum(distance) FROM flight group by flight_date, airline_code;")
total_flight_distance.show()

# debug data, must be log file (*on dev)
# flight.show()
# flight.printSchema()
# total_flight_distance.show()

## write data to biqquery
flight.write.mode('overwrite').format("bigquery") \
    .option("table", f"{PROJECT}:{DATASET}.flight") \
    .save()

total_flight_distance.write.mode('overwrite').format("bigquery") \
    .option("table", f"{PROJECT}:{DATASET}.total_flight_distance") \
    .save()

#gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar