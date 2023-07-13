# Import Glue libraries
import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize job
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET'])
job.init('JOB_NAME', args)

# Define your input & output S3 paths
input_path = args['INPUT_BUCKET']
output_path = args['OUTPUT_BUCKET']

try:
    # Read specific columns from S3 to optimize read operations
    # Read the data into a DynamicFrame
    # dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database = "nyc-curated-db", table_name = "curated_data")
    
    # Convert DynamicFrame to Dataframe
    # df = dynamic_frame.toDF()
    df = spark.read.parquet(input_path)
    
    # Apply the transformation to correct the PULocationID column
    df_corrected = df.withColumn(
        "PULocationID",
        F.when(F.col("PULocationID") == 161, 237)
        .when(F.col("PULocationID") == 237, 161)
        .otherwise(F.col("PULocationID"))
    )
    df_corrected = df_corrected.filter((F.month(F.col("tpep_pickup_datetime")) == 3) & (F.year(F.col("tpep_pickup_datetime")) == 2021))

    df_corrected.write.mode('overwrite').parquet(output_path)
    
except Exception as e:
    print("An error occurred: ", e)
    sys.exit(1) # Stop the job if an error occurred

# Commit job
job.commit()

