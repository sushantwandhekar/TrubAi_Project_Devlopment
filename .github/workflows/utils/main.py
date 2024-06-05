import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3

# Import the utils module
from utils import read_data_from_s3

# Initialize Spark and Glue contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def write_to_redshift(df, redshift_connection_name, table_name, redshift_tmp_dir):
    """
    Write a PySpark DataFrame to Redshift using an existing Glue connection.

    Parameters:
        df (DataFrame): PySpark DataFrame to be written to Redshift.
        redshift_connection_name (str): Name of the Glue connection configured for Redshift.
        table_name (str): Name of the Redshift table to write data into.
        redshift_tmp_dir(str): s3 directory path to redshift temp storage.
        mode (str, optional): Specifies the behavior when data or table already exists. 
            Must be one of "append", "overwrite", "ignore", or "error". 
            Defaults to "overwrite".
    """
    
    # Convert the Spark DataFrame to a Glue DynamicFrame
    output_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "output_dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame, 
        connection_type="redshift", 
        connection_options={"redshiftTmpDir": redshift_tmp_dir, 
                            "useConnectionProperties": "true", 
                            "dbtable": table_name, 
                            "connectionName": redshift_connection_name}, 
        transformation_ctx="redshift_write"
        )
    
    return

def rename_and_cast_columns(df, columns_cast_mappings: list):
    """
    Rename and cast multiple columns in a PySpark DataFrame.

    Parameters:
        df (DataFrame): The original PySpark DataFrame.
        columns_cast_mappings (list): A list of tuples where each tuple contains the old column name and the new column name and the target data type.

    Returns:
        DataFrame: The DataFrame with renamed and casted columns.
    """
    
    # Rename columns
    for old_col, new_col, data_type in columns_cast_mappings:
        df = df.withColumnRenamed(old_col, new_col)
    
    # Cast columns
    for old_col, new_col, data_type in columns_cast_mappings:
        df = df.withColumn(new_col, col(new_col).cast(data_type))
    
    return df

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


