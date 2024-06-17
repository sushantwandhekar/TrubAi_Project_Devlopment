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
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType