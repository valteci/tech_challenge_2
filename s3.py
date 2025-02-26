import boto3
from botocore.exceptions import NoCredentialsError

s3_client = boto3.client('s3')


def upload(
        your_file_path: str,
        bucket_path: str,
        object_name: str = None,
        prefix: str = None
) -> None:

    try:
        if object_name is None:
            object_name = your_file_path.split('/')[-1]
        if prefix:
            object_name = f'{prefix}/{object_name}'

        s3_client.upload_file(your_file_path, bucket_path, object_name)       

    
    except Exception as e:
        print(e)
