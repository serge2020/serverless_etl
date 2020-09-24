""" Lambda function that subscribes to Kinesis Data stream topic and saves its records in csv format S3.
It requires environmental variables
:param MY_AWS_REGION, AWS region where Kinesis stream has been created.
:param STREAM_NAME, the name of Kinesis data stream
:param BUCKET_NAME, name of the target S3 bucket
:param FILE_NAME, base string to use in the exported file name
:param TIME_ZONE, UTC timezone abbreviation to be used as home timezone, eg. 'Europe/Helsinki'
:param RUN_SECONDS, activity time in seconds for Kinesis producer'
"""
import json
import boto3
from botocore.exceptions import ClientError, ParamValidationError
from io import StringIO
import pandas as pd
import os
from datetime import datetime, timedelta
import pytz

aws_region = os.environ['MY_AWS_REGION']
stream_name = os.environ['STREAM_NAME']
bucket_name = os.environ['BUCKET_NAME']
file_name = os.environ['FILE_NAME']
time_zone = os.environ['TIME_ZONE']
run_seconds = int(os.environ['RUN_SECONDS'])

kinesis_client = boto3.client('kinesis', region_name=aws_region)
s3 = boto3.resource('s3')


def lambda_handler(event, context):
    
    def get_kinesis_data_stream(stream_name, seconds_running):
        """ function that subscribes Kinesis consumer to data stream and keeps it active for a defined time period.
        It takes parameters
        :param stream_name: the name of the stream
        :param seconds_running: activity time in seconds for Kinesis consumer
        while running it generates list of kinesis records - one record per shard iterator.
        """
        # get Kinesis stream params - shard id and shard iterator
        try:
            response = kinesis_client.describe_stream(StreamName=stream_name)
        
            my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        
            shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                               ShardId=my_shard_id,
                                                               ShardIteratorType='LATEST')
        
            my_shard_iterator = shard_iterator['ShardIterator']
            end_time = datetime.now() + timedelta(seconds=seconds_running)
        # boto3 error handling using ClientError and ParamValidationError errors.
        except ClientError as e:
            print("Kinesis stream returned error: ", e.response['Error']['Message'])
            raise e
        except ParamValidationError as e:
            raise ValueError(f'The parameters you provided are incorrect: {e}')         
        # get kinesis stream records for as long as activity time limit has been reached
        while True:
            try:
                record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator)
                # Only run for a certain amount of time.
                # Stop looping if no data returned. This means it's done
                now = datetime.now()
    
                if end_time < now:
                    break
                else:
                    # yield data to outside calling iterator
                    for record in record_response['Records']:
                        data = record["Data"].decode("utf-8")
                        timestamp = record["ApproximateArrivalTimestamp"]
                        record_id = record["SequenceNumber"]
                        tweet_record = [record_id, timestamp, data]
                        yield tweet_record
    
                # Get next iterator for shard from previous request
                my_shard_iterator = record_response['NextShardIterator']
    
            # Catch exception meaning hitting API too much
            except ClientError as error:
                if error.response['Error']['Code'] == 'LimitExceededException':
                    print('API call limit exceeded; backing off and retrying...')
                else:
                    raise error

    csv_buffer = StringIO() # memory buffer to store dataframe data
    try:
        # get list of Kinesis records
        kinesis_data = get_kinesis_data_stream(stream_name, run_seconds)
        # store them to Pandas dataframe
        _df_1 = pd.DataFrame(kinesis_data, columns=['record_id', 'timestamp', 'tweet_data'])
        if _df_1.shape[0] > 0:
            # transform tweet_data field from string to json object, then extract values to list
            _df_2 = _df_1.tweet_data.apply(json.loads).values.tolist()
            # create dataframe from extracted values
            _df_3 = pd.DataFrame.from_records(_df_2)
            # add 'record_id' column from initial dataframe to be used in a subsequent join
            _df_3['record_id'] = pd.Series(_df_1['record_id'])
            _df_1 = _df_1.set_index('record_id')
            _df_3 = _df_3.set_index('record_id')
            # join initial dataframe with tweet record dataframe then drop old ''weet_data' column from Kinesis stream
            _df_4 = pd.merge(_df_1, _df_3, how='inner', left_index=True, right_index=True)
            df = _df_4.drop('tweet_data', axis=1)
            # set timezone and fil name variables for saving to S3
            timezone = pytz.timezone(time_zone)
            count_row = df.shape[0]
            record_time = datetime.now(tz=timezone).strftime("%Y-%m-%d_%H%M%S")
            file_name_full = \
                "landing/" + \
                record_time.split("_")[0].split("-")[0] + "/" + \
                record_time.split("_")[0].split("-")[1] + "/" + \
                record_time.split("_")[0].split("-")[2] + "/" + \
                file_name + "-" + record_time.split("_")[1] + ".csv"
            # save dataframe as csv file to S3
            df.to_csv(csv_buffer, header=False)
            s3.Object(bucket_name, file_name_full).put(Body=csv_buffer.getvalue())
            print(f"{count_row} records saved to CSV file on S3")
            kinesis_data = []
        else:
            print("No new records received from Kinesis data stream")

    # boto3 error handling using ClientError error.
    except ClientError as e:
            print("Kinesis stream returned error: ", e.response['Error']['Message'])
            raise e    

    return json.dumps({'exit_status':'SUCCESS'})
