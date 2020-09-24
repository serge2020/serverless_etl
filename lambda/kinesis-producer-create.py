""" Lambda function that creates Kinesis Data Stream. Requires environmental variable
:param STREAM_NAME, the name of Kinesis data stream.
"""
import json
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import os
import time

stream_name = os.environ['STREAM_NAME']
k_client = boto3.client('kinesis')


def lambda_handler(event, context):
    
    print("Stream_name: ", stream_name)
    try:
        k_client.create_stream(StreamName=stream_name, ShardCount=1)
        print("creating Kinesis stream {0} ".format(stream_name))
    # boto3 error handling using ClientError and ParamValidationError errors.
    except ClientError as e:
        print("Kinesis stream returned error: ", e.response['Error']['Message'])
        # handling cases when the stream already exists
        if e.response['Error']['Code'] != 'ResourceInUseException':
            raise e
    except ParamValidationError as e:
        raise ValueError(f'The parameters you provided are incorrect: {e}')
        
    try:    
        stream_info = k_client.describe_stream(StreamName=stream_name)
        stream_desc = stream_info["StreamDescription"]
        stream_status = stream_desc["StreamStatus"]
        while stream_status != "ACTIVE":
            time.sleep(3)
            stream_info = k_client.describe_stream(StreamName=stream_name)
            stream_desc = stream_info["StreamDescription"]
            stream_status = stream_desc["StreamStatus"]
            print("{0} status is {1}".format(stream_name, stream_status))
    except ClientError as e:
        print("Kinesis stream returned error: ", e.response['Error']['Message'])
        raise e
    except ParamValidationError as e:
        raise ValueError(f'The parameters you provided are incorrect: {e}')
        
    return json.dumps({"exit_status":"SUCCESS"})