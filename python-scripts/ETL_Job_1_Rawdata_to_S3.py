import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import requests
import boto3
import hashlib

# Create a GlueContext and SparkContext
glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session

# Initialize job parameters
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET'])
job.init(args['JOB_NAME'], args)

# Define your input & output S3 paths
input_path = args['INPUT_BUCKET']
output_path = args['OUTPUT_BUCKET']

# Set the URL of the file to download
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-03.parquet'

# Set the S3 bucket and key to save the downloaded file
bucket_name = 'nyc-bronze'
file_key = '2021/march/yellow_tripdata_2021-03.parquet'

# Download the file from the URL
response = requests.get(url)
online_file_content = response.content

# Calculate MD5 hash of the online file
online_file_md5_hash = hashlib.md5(online_file_content).hexdigest()

# Write the file content to S3 using AWS Glue
file_writer_client = boto3.client('s3')
file_writer_client.put_object(Body=online_file_content, Bucket=input_path, Key=output_path)

print(f"File '{file_key}' successfully downloaded and uploaded to S3 bucket '{bucket_name}'.")

response = file_writer_client.get_object(Bucket=bucket_name, Key=file_key)
s3_file_content = response['Body'].read()
s3_file_md5_hash = hashlib.md5(s3_file_content).hexdigest()

# check if both hashes are equal
if online_file_md5_hash == s3_file_md5_hash:
    print("File is not corrupted")
else:
    print("File is corrupted")
    sys.exit(1)
    
# Commit the Glue job
job.commit()
