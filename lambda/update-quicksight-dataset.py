
""" Lambda function that initialises AWS Quicksight dataset ingestion for analytical.hashtag_data dataset
zone. It requires environmental variables
:param ACCOUNT_ID, user account id.
:param QS_DATASET_ID, id of the Quicksight dataset
:param TIME_ZONE, UTC timezone abbreviation to be used as home timezone, eg. 'Europe/Helsinki'
"""
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import json
import os
from datetime import datetime
import pytz
import time


account_id = os.environ['ACCOUNT_ID']
dataset_id = os.environ['QS_DATASET_ID']
timezone = pytz.timezone(os.environ['TIME_ZONE'])

def lambda_handler(event, context):
    # initiate service client
    client = boto3.client('quicksight')
    ingestion_id = datetime.now(tz=timezone).strftime("%Y-%m-%d_%H%M%S")
    # run ingestion job
    client.create_ingestion(DataSetId=dataset_id, IngestionId=ingestion_id,
                            AwsAccountId=account_id)
    # wait for ingestion job is completed
    while True:
        try:
            response = client.describe_ingestion(DataSetId=dataset_id,
                                                 IngestionId=ingestion_id,
                                                 AwsAccountId=account_id)
            if response['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING'):
                time.sleep(5)
            elif response['Ingestion']['IngestionStatus'] == 'COMPLETED':
                print(
                    "refresh completed. RowsIngested {0}, RowsDropped {1}, IngestionTimeInSeconds {2}, IngestionSizeInBytes {3}".format(
                        response['Ingestion']['RowInfo']['RowsIngested'],
                        response['Ingestion']['RowInfo']['RowsDropped'],
                        response['Ingestion']['IngestionTimeInSeconds'],
                        response['Ingestion']['IngestionSizeInBytes']))
                break
            else:
                print("refresh failed for {0}! - status {1}".format(dataset_id, response['Ingestion']['IngestionStatus']))
                print("Error info: ", response['Ingestion']['ErrorInfo']['Type'])
                print("Error message: ", response['Ingestion']['ErrorInfo']['Message'])
                break
        # boto3 error handling using ClientError and ParamValidationError errors.
        except ClientError as e:
            print("Glue client returned error: ", e.response['Error']['Message'])
            raise e
        except ParamValidationError as e:
            raise ValueError(f'The parameters you provided are incorrect: {e}')
   
    return json.dumps({"exit_status":"SUCCESS"})
