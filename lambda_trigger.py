import json
import boto3
import urllib.parse

# Initialize the AWS Glue client
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    # 1. Automatically extract the uploaded file path from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Construct the full S3 path of the specific file that was just uploaded
    s3_file_path = f"s3://{bucket}/{key}"
    print(f"New file detected and being processed: {s3_file_path}")
    
    try:
        # 2. Start the AWS Glue ETL Job
        response = glue_client.start_job_run(
            JobName='glue_etl',  # Your exact AWS Glue job name
            Arguments={
                '--new_s3_file': s3_file_path  # Passes the dynamic file path to Glue
            }
        )
        
        # 3. Safely extract the dynamic Job Run ID from AWS's response dictionary
        job_run_id = response['JobRunId']
        print(f"Successfully triggered Glue Job 'glue_etl'. Run ID: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue job started with Run ID: {job_run_id}")
        }
        
    except Exception as e:
        print(f"Error starting Glue job: {str(e)}")
        raise e
