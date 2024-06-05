import boto3

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
                # '--job-language': 'python',
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

def main():
    list_connec= ['Redshift connection_trubai_dw']
    extra_files = 's3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/utils.zip'
    job_name = "automated_glue_job"
    script_location = "s3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/main.py" # Replace with your S3 path to the zip folder
    role_arn = "arn:aws:iam::311373145380:role/trubai_dev_glue_role" # Replace with your Glue service role ARN
    region_name = "us-east-1" # Replace with your AWS region

    glue_client = boto3.client('glue', region_name=region_name)

    create_glue_job(job_name, script_location, role_arn, glue_client,list_connec,extra_files)

if __name__ == "__main__":
    main()
