# transit zone elt clone 
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def read_data_from_s3(spark,s3_path, file_format, file_options):
    # Read data into a Spark DataFrame
    df = spark.read.format(file_format).options(**file_options).load(s3_path)
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




file_format = "csv"
options_csv = {
    "header": "true",
    
    "inferSchema": "true",
    "delimiter": ","
}

transit_zone_path = "s3://data-ingestion-bucket-trubai-dev/trubai-gov-data-engineering/raw/transit_zones/nyctransitzones_201601 (2).csv"

casting = [
("the_geom", "the_geom", "geometry"),
("Shape_Leng","shape_leng","double"),
("Shape_Area","shape_area","double")  
]

redshift_tmp_dir = "s3://aws-glue-assets-311373145380-us-east-1/temporary/"
redshift_connection_name = "Redshift connection_trubai_dw"
redshift_table_name_transit_zonet = "transit_zone.raw_transit_zone"

def transit_zone():
    df_tran = read_data_from_s3(spark,transit_zone_path,file_format, options_csv)
    df_tran = rename_and_cast_columns(df_tran, casting)
    write_to_redshift(df_tran, redshift_connection_name, redshift_table_name_transit_zonet, redshift_tmp_dir)


transit_zone()




job.commit()
