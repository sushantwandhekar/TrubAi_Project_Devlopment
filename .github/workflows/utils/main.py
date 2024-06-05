import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# Import the utils module
from utils import read_data_from_s3

# Initialize Spark and Glue contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Example: Use the utility function to read data from S3
s3_path = "s3://data-ingestion-bucket-trubai-dev/trubai-gov-data-engineering/raw/neighborhood/NHoodNameCentroids.csv"
file_format = "csv"
options_csv = {
    "header": "true",
    "inferSchema": "true",
    "delimiter": ","
}
df = read_data_from_s3(spark,s3_path, file_format, options_csv)

# Print schema and a few rows for debugging
df.printSchema()
df.show(5)

job.commit()


