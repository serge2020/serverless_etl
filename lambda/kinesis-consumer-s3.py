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
tweet_cols = os.environ['TWEET_COLS']
cols = tweet_cols.split(',')
id_col = cols[0]
old_col = cols[-1]


def lambda_handler(event, context):

    def boto_safe_run(func):
        """
        decorator (higher order function) to handle errors using boto3 using ClientError and ParamValidationError error types
        :param func: outer function to be passed to decorator
        :return: executed inner function
        """
        def inner_function(*args, **kwargs):
            # inner function that uses arguments of the outer function runs it applying error handling functionality
            try:
                return func(*args, *kwargs)
            except ClientError as e:
                print("AWS client returned error: ", e.response['Error']['Message'])
                raise e
            except ParamValidationError as e:
                raise ValueError(f'The parameters you provided are incorrect: {e}')
        return inner_function

    @ boto_safe_run
    def get_kinesis_shard_iterator(client, stream_name):
        """ function that subscribes Kinesis consumer to data stream and keeps it active for a defined time period.
        It takes parameters
        :param stream_name: the name of the stream
        while running it generates list of kinesis records - one record per shard iterator.
        """
        # get Kinesis stream params - shard id and shard iterator
        response = client.describe_stream(StreamName=stream_name)
        my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                                           ShardId=my_shard_id,
                                                           ShardIteratorType='LATEST')
        my_shard_iterator = shard_iterator['ShardIterator']
        return my_shard_iterator

    @ boto_safe_run
    def subscribe_to_stream(client, iterator, seconds_running):
        """ function that subscribes Kinesis consumer to data stream and keeps it active for a defined time period.
        It takes parameters
        :param stream_name: the name of the stream
        :param seconds_running: activity time in seconds for Kinesis consumer
        while running it generates list of kinesis records - one record per shard iterator.
        """
        end_time = datetime.now() + timedelta(seconds=seconds_running)
        while True:
            record_response = client.get_records(ShardIterator=iterator)
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
            iterator = record_response['NextShardIterator']

    def tweet_to_df(data_frame, id_col):
        # transform tweet_data field from string to json object, then extract values to list
        _df_1 = data_frame.tweet_data.apply(json.loads).values.tolist()
        # create dataframe from extracted values
        _df_2 = pd.DataFrame.from_records(_df_1)
        # add id column from initial dataframe to be used in a subsequent join
        _df_2.insert(0, id_col, pd.Series(data_frame[id_col]))
        return _df_2

    def merge_df(df1, df2, index_col, drop_col):
        df1 = df1.set_index(index_col)
        df2 = df2.set_index(index_col)
        df3 = pd.merge(df1, df2, how='inner', left_index=True, right_index=True)
        df = df3.drop(drop_col, axis=1)
        return df

    @boto_safe_run
    def save_df_to_s3(df, s3_resource, bucket, filename):
        """ function that saves contents of a dataframe to S3
        :param df: source dataframe
        :param s3_resource: S£resource client
        :param bucket: target bucket name
        :param filename: target file key (name)
        :return: None
        """
        csv_buffer = StringIO() # memory buffer to store dataframe data
        df.to_csv(csv_buffer, header=False)
        s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())

    def get_file_name(tz, filename):
        timezone = pytz.timezone(tz)
        record_time = datetime.now(tz=timezone).strftime("%Y-%m-%d_%H%M%S")
        _file_name = \
            "landing/" + \
            record_time.split("_")[0].split("-")[0] + "/" + \
            record_time.split("_")[0].split("-")[1] + "/" + \
            record_time.split("_")[0].split("-")[2] + "/" + \
            filename + "-" + record_time.split("_")[1] + ".csv"
        return _file_name

    kinesis_client = boto3.client('kinesis', region_name=aws_region)
    s3 = boto3.resource('s3')

    my_iterator = get_kinesis_shard_iterator(kinesis_client, stream_name)
    kinesis_data = subscribe_to_stream(kinesis_client, my_iterator, run_seconds)
    df_1 = pd.DataFrame(kinesis_data, columns=cols)
    if df_1.shape[0] > 0:
        df_2 = tweet_to_df(df_1, id_col)
        df_3 = merge_df(df_1, df_2, id_col, old_col)
        full_name = get_file_name(time_zone, file_name)
        save_df_to_s3(df_3, s3, bucket_name, full_name)
        count_row = df_3.shape[0]
        print(f"{count_row} records saved to CSV file on S3")
    else:
        print("No new records received from Kinesis data stream")

    return json.dumps({'exit_status':'SUCCESS'})
