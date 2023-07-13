import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define your input & output S3 paths
input_path = args['INPUT_BUCKET']
output_path = args['OUTPUT_BUCKET']

try:
    # Script generated for node S3 bucket
    S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [input_path],
            "recurse": True,
        },
        transformation_ctx="S3bucket_node1",
    )

    # Script generated for node AWS Glue Data Catalog
    AWSGlueDataCatalog_node1689085085428 = glueContext.write_dynamic_frame.from_catalog(
        frame=S3bucket_node1,
        database="nyc-curated-db",
        table_name="test_nyccrukdb_public_nyctripdata",
        transformation_ctx="AWSGlueDataCatalog_node1689085085428",
    )
except Exception as e:
    print("An error occurred: ", e)
    sys.exit(1) # Stop the job if an error occurred


job.commit()
