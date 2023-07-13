import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Create a Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET'])
job.init(args['JOB_NAME'], args)

# Define your input & output S3 paths
input_path = args['INPUT_BUCKET']
output_path = args['OUTPUT_BUCKET']

try:

    # Read data from S3
    df = spark.read.parquet(input_path)

    # Select needed columns
    df = df.select('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'PULocationID')

    # Get trip duration in hours for trip speed calculation
    df = df.withColumn("trip_duration", (F.col("tpep_dropoff_datetime").cast("long") -F.col("tpep_pickup_datetime").cast("long")) / 3600)

    # Filter out negative and 0 data
    df = df.filter(F.col("trip_duration") >= 0)

    # Write data back to S3
    df.write.mode('overwrite').parquet(output_path)

except Exception as e:
    print("An error occurred: ", e)
    sys.exit(1) # Stop the job if an error occurred



# Finalize your job so AWS Glue updates the job bookmark.
job.commit()
