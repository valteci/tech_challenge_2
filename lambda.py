import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('glue')

    response = client.start_job_run(
        JobName = 'etl-b3-job-copy'
    )

    return {
        'statusCode': 200,
        'body': f'job Glue iniciado com sucesso: {response["JobRunId"]}'
    }