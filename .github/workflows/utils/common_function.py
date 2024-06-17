from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3
import json
import sys
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
logger = logging.getLogger(__name__)
# logger.info(args)
from pyspark.sql.functions import col
def read_data_from_s3(spark,file_path, file_format, file_options):
    if file_format.lower() == 'csv':
        logger.info('Observed CSV file format')
        df = spark.read.options(**file_options).csv(file_path)
        df.printSchema()
    elif file_format.lower() == 'parquet':
        logger.info('Observed Parquet file format')
        df = spark.read.parquet(file_path)
        df.printSchema()
    else:
        raise ValueError("Unsupported file format")
    return df

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

def rename_and_cast_columns(df, columns_cast_mappings):
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

def convert_to_timestamp(df, datetime_columns):
    for old_col, new_col, data_type in datetime_columns:
        df = df.withColumnRenamed(old_col, new_col)
    
    # Cast columns
    for old_col, new_col, data_type in datetime_columns:
        df = df.withColumn(new_col, to_timestamp(col(new_col),'MM/dd/yyyy hh:mm:ss a'))
    return df
