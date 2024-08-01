import sys
import os
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from utils.common_function import read_data_from_s3,write_to_redshift,convert_to_timestamp,rename_and_cast_columns,get_secret,camel_to_snake,rename_columns
import logging
logger = logging.getLogger(__name__)
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SECRET_NAME = 'dev/trubai/rds-deployment-user'
REGION_NAME = 'us-east-1'

session = boto3.session.Session()
print(session)

client = session.client(
    service_name='secretsmanager',
    region_name=REGION_NAME
)

def get_secret():
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=SECRET_NAME
        )
    except Exception as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

file_format ='csv'
file_options = {
    "header": "true",
    "inferSchema": "true",
    "delimiter": ","
}

def write_rds_postgres(df,jdbc_url,DB_TABLE,DB_USER,DB_PASSWORD):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", DB_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

secret = get_secret()
print(secret)

# PostgreSQL connection details
DB_HOST = secret['host']
DB_PORT = secret['port']
DB_NAME = secret['db_name']
DB_USER = secret['username']
DB_PASSWORD = secret['password']
DB_TABLE_city = 'trubai_gov_ai_application.city'



jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
connection_options = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}
print('Connection_successful:',jdbc_url)

file_path_country = 's3://data-ingestion-bucket-trubai-dev/trubai-gov-data-engineering/raw/country/countries.csv'
df_country = read_data_from_s3(spark,file_path_country, file_format, file_options)
df_country = df_country.withColumnRenamed('id','country_country_id').\
                withColumnRenamed('name','country_name').\
                withColumnRenamed('region_id','continent_id').\
                withColumnRenamed('subregion_id','sub_continent_id').\
                withColumnRenamed('iso2','country_code').\
                select('country_country_id','country_name','country_code','phone_code','nationality','continent_id','sub_continent_id')
                
file_path_states = 's3://data-ingestion-bucket-trubai-dev/trubai-gov-data-engineering/raw/states/states.csv'
df_states = read_data_from_s3(spark,file_path_states, file_format, file_options)
df_states = df_states.withColumnRenamed('id','state_state_id').\
                withColumnRenamed('name','state_name').\
                withColumnRenamed('country_id','state_country_id').\
                withColumnRenamed('latitude','state_latitude').\
                withColumnRenamed('longitude','state_longitude').\
                select('state_state_id','state_name','state_latitude','state_longitude','state_country_id','state_code')  
                
file_path_city = 's3://data-ingestion-bucket-trubai-dev/trubai-gov-data-engineering/raw/cities/cities.csv'
df_cities = read_data_from_s3(spark,file_path_city, file_format, file_options)
df_cities = df_cities.withColumnRenamed('id', 'city_id').\
            withColumnRenamed('name', 'city_name').\
            withColumn('redshift_schema_name', lit(False)).\
            withColumn('is_launched',lit(False)).\
            select('city_id','city_name','redshift_schema_name','latitude','longitude','is_launched','country_id','state_id')
            
df_cities_new = df_cities.join(df_states, df_cities.state_id == df_states.state_state_id,'left')
df_cities_new = df_cities_new.join(df_country, df_cities_new.country_id == df_country.country_country_id, 'left')
df_cities = df_cities_new.select('city_id','city_name','redshift_schema_name','latitude','longitude','state_id','country_id','is_launched')

write_rds_postgres(df_cities,jdbc_url,DB_TABLE_city,DB_USER,DB_PASSWORD)



job.commit()