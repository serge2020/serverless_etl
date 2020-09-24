""" Lambda function that creates Kinesis Data Stream. Requires environmental variable
:param STREAM_NAME, the name of Kinesis data stream.
"""
import json
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import os

stream_name = os.environ['STREAM_NAME']
k_client = boto3.client('kinesis')


def lambda_handler(event, context):
    
    print("Stream_name: ", stream_name)
    
    try:
        k_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
    # boto3 error handling using ClientError and ParamValidationError errors.
    except ClientError as e:
        print("Kinesis stream returned error: ", e.response['Error']['Message'])
        raise e
    except ParamValidationError as e:
        raise ValueError(f'The parameters you provided are incorrect: {e}')    
    
    deleted = False
    while not deleted:
        try:
            response = k_client.list_streams(Limit=20, ExclusiveStartStreamName=stream_name)
            _stream = response["StreamNames"][0]
            if _stream == stream_name:
                deleted = False
            else:
                deleted = True
        except IndexError:
            deleted = True

    print(f"data stream {stream_name} DELETED")
    
    return json.dumps({'exit_status':'SUCCESS'})