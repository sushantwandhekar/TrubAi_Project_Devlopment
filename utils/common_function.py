from awsglue.transforms import *
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import pandas as pd
import logging
import json
import re
import boto3

# Initialize logger
logger = logging.getLogger(__name__)

def read_data_from_s3(spark,file_path, file_format, file_options=None):
    if file_options is None:
        file_options = {}

    if file_format.lower() == 'csv':
        logger.info('Observed CSV file format')
        df = spark.read.options(**file_options).csv(file_path)
    elif file_format.lower() == 'parquet':
        logger.info('Observed Parquet file format')
        df = spark.read.parquet(file_path)
    elif file_format.lower() in ['xls', 'xlsx', 'excel']:
        logger.info(f'Observed {file_format.upper()} file format')
        # Read Excel file with pandas
        df = pd.read_excel(file_path, **file_options)
    else:
        raise ValueError("Unsupported file format")
    
    # df.printSchema()
    return df


def write_to_redshift(glueContext,df, redshift_connection_name, table_name, redshift_tmp_dir):
    """
    Write a PySpark DataFrame to Redshift using an existing Glue connection.

    Parameters:
        df (DataFrame): PySpark DataFrame to be written to Redshift.
        redshift_connection_name (str): Name of the Glue connection configured for Redshift.
        table_name (str): Name of the Redshift table to write data into.
        redshift_tmp_dir(str): s3 directory path to redshift temp storage.
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

def rename_columns(df, columns_cast_mappings):
    """
    Rename columns in the DataFrame based on the provided list of old and new column names.
    
    Parameters:
    df (DataFrame): Spark DataFrame whose columns are to be renamed.
    columns_cast_mappings (list): List of tuples containing old column name, new column name, 
                                  and optionally data type.
    
    Returns:
    DataFrame: DataFrame with columns renamed.
    """
    for mapping in columns_cast_mappings:
        if len(mapping) == 2:
            old_column, new_column = mapping
        elif len(mapping) == 3:
            old_column, new_column, _ = mapping
        else:
            print(f"Warning: Invalid mapping {mapping}.")
            continue

        if old_column in df.columns:
            df = df.withColumnRenamed(old_column, new_column)
        else:
            print(f"Warning: Column {old_column} does not exist in the DataFrame.")
    return df

def cast_columns(df, columns_cast_mappings):
    for old_col, new_col, data_type in columns_cast_mappings:
        df = df.withColumn(new_col, col(new_col).cast(data_type))
    return df

def convert_to_timestamp(df, datetime_columns):
    # Cast columns
    for old_col, new_col, data_type in datetime_columns:
        df = df.withColumn(new_col, to_timestamp(col(new_col),'MM/dd/yyyy hh:mm:ss a'))
    return df

def get_secret():
    """
    Function will return the secrets fetched from the secret_name
    """
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=SECRET_NAME
        )
    except Exception as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)
    

def camel_to_snake(name):
    """
    Convert camelCase or PascalCase to snake_case.
    """
    # Step 1: Normalize underscores and dashes to underscores
    name = name.replace("-", "_")
    
    # Step 2: Insert underscore between number and capital letter
    name = re.sub(r'([0-9])([A-Z])', r'\1_\2', name)
    
    # Step 3: Insert underscore before each capital letter, if not already there
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    
    # Step 4: Convert entire string to lowercase
    name = name.lower()
    print(name)
    # Step 5: Return the snake-cased string
    return name
    
