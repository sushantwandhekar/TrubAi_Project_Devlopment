import boto3
import hashlib
import os
import time
import sys

def job_exists(glue_client, job_name):
    """
    Check if the Glue job with the given name exists.

    Parameters:
    glue_client (boto3.client): The Boto3 Glue client.
    job_name (str): The name of the Glue job.

    Returns:
    bool: True if the job exists, False otherwise.
    dict or None: Job details if it exists, None otherwise.
    """
    try:
        response = glue_client.get_job(JobName=job_name)
        return True, response['Job']
    except glue_client.exceptions.EntityNotFoundException:
        return False, None


def create_glue_job(job_name, script_location, role_arn, glue_client, glue_connection_list, extra_python_files):
    """
    Create a new Glue job.

    Parameters:
    job_name (str): The name of the Glue job.
    script_location (str): The S3 path where the job script is located.
    role_arn (str): The ARN of the IAM role to be used by the Glue job.
    glue_client (boto3.client): The Boto3 Glue client.
    glue_connection_list (list): List of Glue connection names.
    extra_python_files (str): S3 path to additional Python files.

    Returns:
    None
    """
    try:
        response = glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': job_name + '.py',
                'ScriptLocation': script_location,
                'PythonVersion': '3' 
            },
            DefaultArguments={
                '--job-language': 'python',
                '--extra-py-files': extra_python_files
            },
            Connections={
                'Connections': glue_connection_list
            },
            GlueVersion='4.0',
            WorkerType='G.8X',
            NumberOfWorkers=10,
            Timeout=120,
            MaxRetries=0
        )
        print("Glue ETL Job created successfully:", response['Name'])
    except Exception as e:
        print("Error creating Glue ETL Job:", str(e))

def update_glue_job(job_name, script_location, role_arn, glue_client, glue_connection_list, extra_python_files):
    """
    Update an existing Glue job.

    Parameters:
    job_name (str): The name of the Glue job.
    script_location (str): The S3 path where the job script is located.
    role_arn (str): The ARN of the IAM role to be used by the Glue job.
    glue_client (boto3.client): The Boto3 Glue client.
    glue_connection_list (list): List of Glue connection names.
    extra_python_files (str): S3 path to additional Python files.

    Returns:
    None
    """
    try:
        response = glue_client.update_job(
            JobName=job_name,
            JobUpdate={
                'Role': role_arn,
                'Command': {
                    'Name': job_name + '.py',
                    'ScriptLocation': script_location,
                    'PythonVersion': '3'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--extra-py-files': extra_python_files
                },
                'Connections': {
                    'Connections': glue_connection_list
                },
                'WorkerType': 'G.8X',
                'NumberOfWorkers': 10,
                'GlueVersion': '4.0',
                'Timeout': 120,
            }
        )
        print("Glue ETL Job updated successfully:", response['JobName'])
    except Exception as e:
        print("Error updating Glue ETL Job:", str(e))

def calculate_s3_object_md5(s3_client, bucket_name, key):
    """
    Calculate the MD5 hash of an object stored in an S3 bucket.

    Parameters:
    s3_client (boto3.client): The Boto3 S3 client object.
    bucket_name (str): The name of the S3 bucket.
    key (str): The key (path) of the object in the S3 bucket.

    Returns:
    str: The MD5 hash of the object content.
    """

    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    md5 = hashlib.md5(obj['Body'].read()).hexdigest()
    return md5

def calculate_file_md5(file_path):
    """
    Calculate the MD5 hash of a local file.

    Parameters:
    file_path (str): The path to the local file.

    Returns:
    str: The MD5 hash of the file content.
    """

    with open(file_path, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    return md5


def main(job_name):
    """
    Main function to orchestrate Glue job creation or update.

    Parameters:
    job_name (str): The name of the Glue job.

    Returns:
    None
    """
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
    glue_connection_list = os.getenv('GLUE_CONNECTION_LIST').split(',')
    extra_python_files = os.getenv('EXTRA_PYTHON_FILES')
    script_location = f"s3://{s3_bucket_name}/glue_cicd_automation/ETL_Jobs/{job_name}.py"
    role_arn = os.getenv('ROLE_ARN')
    region_name = os.getenv('REGION_NAME')

    s3_key = f"glue_cicd_automation/ETL_Jobs/{job_name}.py"
    local_script_path = f"jobs/{job_name}.py"
    glue_client = boto3.client('glue', region_name=region_name)
    s3_client = boto3.client('s3', region_name=region_name)
    
    # Check if the job already exists
    exists, job_info = job_exists(glue_client, job_name)

    if not exists:
        # Job does not exist, create it
        create_glue_job(job_name, script_location, role_arn, glue_client, glue_connection_list, extra_python_files)
    else:
        try:
            current_script_md5 = calculate_s3_object_md5(s3_client, s3_bucket_name, s3_key)
            new_script_md5 = calculate_file_md5(local_script_path)
            
            if current_script_md5 != new_script_md5:
                # Update the job with the new script location
                update_glue_job(job_name, script_location, role_arn, glue_client, glue_connection_list, extra_python_files)
            else:
                print("Job already exists with the same script. No update needed.")
        except Exception as e:
            print("Error comparing scripts:", str(e))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python create_glue_job.py <job_name>")
        sys.exit(1)
    
    job_name = sys.argv[1]
    main(job_name)
