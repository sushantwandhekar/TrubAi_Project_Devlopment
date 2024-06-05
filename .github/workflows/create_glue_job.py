import boto3
import hashlib

def job_exists(glue_client, job_name):
    try:
        response = glue_client.get_job(JobName=job_name)
        return True, response['Job']
    except glue_client.exceptions.EntityNotFoundException:
        return False, None


def create_glue_job(job_name, script_location, role_arn, glue_client,list_connec,extra_files):
    try:
        response = glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': 'main.py',
                'ScriptLocation': script_location,
                'PythonVersion': '3' 
            },
            DefaultArguments={
                '--job-language': 'python',
                '--extra-py-files': extra_files
            },
            Connections={
                'Connections': list_connec
            },GlueVersion='2.0',
            WorkerType='G.1X',
            NumberOfWorkers=2,
            Timeout=120,
            MaxRetries=0
        )
        print("Glue ETL Job created successfully:", response['Name'])
    except Exception as e:
        print("Error creating Glue ETL Job:", str(e))

def update_glue_job(job_name,script_name, script_location, role_arn, glue_client,list_connec,extra_files):
    try:
        response = glue_client.update_job(
            JobName=job_name,
            JobUpdate={
                'Role': role_arn,
                'Command': {
                    'Name': job_name,
                    'ScriptLocation': script_location,
                    'PythonVersion': '3'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--extra-py-files': extra_files
                },
                'Connections':{
                    'Connections': list_connec
                },'WorkerType': 'G.1X',
                'NumberOfWorkers': 2,
                'GlueVersion': '2.0',
                'Timeout': 120,
            }
        )
        print("Glue ETL Job updated successfully:", response['JobName'])
    except Exception as e:
        print("Error updating Glue ETL Job:", str(e))
        
def calculate_s3_object_md5(s3_client, bucket_name, key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    md5 = hashlib.md5(obj['Body'].read()).hexdigest()
    return md5

def calculate_file_md5(file_path):
    with open(file_path, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    return md5


# def main():
#     list_connec= ['Redshift connection_trubai_dw']
#     extra_files = 's3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/utils.zip'
#     job_name = "automated_glue_job"
#     script_location = "s3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/main.py" # Replace with your S3 path to the zip folder
#     role_arn = "arn:aws:iam::311373145380:role/trubai_dev_glue_role" # Replace with your Glue service role ARN
#     region_name = "us-east-1" # Replace with your AWS region
#     glue_client = boto3.client('glue', region_name=region_name)
#     create_glue_job(job_name, script_location, role_arn, glue_client,list_connec,extra_files)

def main():
    list_connec= ['Redshift connection_trubai_dw']
    extra_files = 's3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/utils.zip'
    job_name = "main"
    script_location = "s3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/main.py"
    role_arn = "arn:aws:iam::311373145380:role/trubai_dev_glue_role"
    region_name = "us-east-1"
    
    s3_bucket_name = "data-ingestion-bucket-trubai-dev"
    s3_key = "glue_cicd_automation/main.py"
    local_script_path = ".github/workflows/utils/main.py"

    glue_client = boto3.client('glue', region_name=region_name)
    s3_client = boto3.client('s3', region_name=region_name)
    
    # Check if the job already exists
    exists, job_info = job_exists(glue_client, job_name)

    if not exists:
        # Job does not exist, create it
        create_glue_job(job_name, script_location, role_arn,glue_client,list_connec, extra_files)
    else:
        # Job exists, check if the script is updated
        # current_script_location = job_info['Command']['ScriptLocation']
        # if current_script_location != script_location:
        #     # Update the job with the new script location
        #     update_glue_job(job_name, script_location, role_arn,glue_client,list_connec,extra_files)
        # else:
        #     print("Job already exists with the same script. No update needed.")
        try:
            current_script_md5 = calculate_s3_object_md5(s3_client, s3_bucket_name, s3_key)
            print(current_script_md5)
            new_script_md5 = calculate_file_md5(local_script_path)
            print(new_script_md5)
            if current_script_md5 != new_script_md5:
                # Update the job with the new script location
                update_glue_job(job_name, script_location, role_arn,glue_client,list_connec,extra_files)
            else:
                print("Job already exists with the same script. No update needed.")
        except Exception as e:
            print("Error comparing scripts:", str(e))


if __name__ == "__main__":
    main()
