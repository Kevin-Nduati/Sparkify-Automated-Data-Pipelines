import logging
import boto3
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):
    """
    This function uploads a file to an S3 bucket

    **Parameters**
    * file_name : file to upload
    * bucket : destination bucket
    * object_name : s3 object name
    """

    if object_name is None:
        object_name = file_name

    # upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
    
s3 = boto3.client('s3')

with open("data/log_data_jsonpaths.json", 'rb') as f:
    s3.upload_fileobj(f, 'kevbucket100', 'log_data_jsonpaths.json')

with open("data/song_data_jsonpaths.json", 'rb') as f:
    s3.upload_fileobj(f, 'kevbucket100', 'song_data_jsonpaths.json')

