import boto3

def create_glue_job(job_name, script_location, role_arn, glue_client):
    try:
        response = glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location
            },
            DefaultArguments={
                '--job-language': 'python'
            },
            Timeout=240,
            MaxCapacity=5.0
        )
        print("Glue ETL Job created successfully:", response['Name'])
    except Exception as e:
        print("Error creating Glue ETL Job:", str(e))

def main():
    job_name = "create_glue_job"
    script_location = "s3://data-ingestion-bucket-trubai-dev/glue_cicd_automation/utils.zip" # Replace with your S3 path to the zip folder
    role_arn = "arn:aws:iam::311373145380:role/trubai_dev_glue_role" # Replace with your Glue service role ARN
    region_name = "us-east-1" # Replace with your AWS region

    glue_client = boto3.client('glue', region_name=region_name)

    create_glue_job(job_name, script_location, role_arn, glue_client)

if __name__ == "__main__":
    main()
