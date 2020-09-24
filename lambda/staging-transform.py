""" Lambda function that loads data from the S3 Landing zone applies necessary transformations and and saves it to Staging
zone. Function also return indicators of data processing to be used by update-data-log Lambda. It requires environmental variables
:param ACCOUNT_ID, user account id.
:param TARGET_DB, the name of target (Staging) database in Glue Catalog
:param TARGET_TABLE, the name of target (Staging) table in Glue Catalog
:param SOURCE_DB, the name of source (Landing) database in Glue Catalog
:param SOURCE_TABLE, the name of source (Landing) table in Glue Catalog
:param BUCKET_NAME, the name of S3 bucket where all data is stored
:param LANDING_PATH, S3 path of the Landing zone
:param STAGING_PATH, S3 path of the Staging zone
:param STAGING_FILE, base string to use in the exported file name
:param TIME_ZONE, UTC timezone abbreviation to be used as home timezone, eg. 'Europe/Helsinki'
:param TIME_HORIZONT_HRS, time filter to apply when loading daily data from Landing zone'
"""
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import json
import os
import pandas as pd
import numpy as np
import base64
import hashlib
from datetime import datetime
from dateutil.parser import *
import pytz
from io import StringIO
from textblob import TextBlob
import re


account_id = os.environ['ACCOUNT_ID']
target_db = os.environ['TARGET_DB']
target_table = os.environ['TARGET_TABLE']
source_db = os.environ['SOURCE_DB']
source_table = os.environ['SOURCE_TABLE']
bucket_name = os.environ['BUCKET_NAME']

landing_path = os.environ['LANDING_PATH']
staging_path = os.environ['STAGING_PATH']
staging_file = os.environ['STAGING_FILE']
timezone = pytz.timezone(os.environ['TIME_ZONE'])
time_horizon = int(os.environ['TIME_HORIZONT_HRS'])

def lambda_handler(event, context):
    
    def get_glue_schema(client, id, db, table, partitioned):
        """ function that retrieves table schema information from Glue Catalog. It requires the following input
        :param client: Glue client
        :param id: Glue Catalog id
        :param db: the name of the database in Glue Catalog
        :param table: the name of the table in Glue Catalog
        :param partitioned: boolean variable indicating if the table is partitioned
        :return: list of table column names
        """
        try:
            # get table metadata from Glue Catalog
            response_stg = client.get_table(CatalogId=id, DatabaseName=db, Name=table)
            if partitioned:
                col_list_stg = []
                part_list = []
                # get partition and column list from Glue Catalog
                columns = response_stg['Table']['StorageDescriptor']['Columns']
                part_columns = response_stg['Table']['PartitionKeys']
                for col in columns:
                    col_list_stg.append(col['Name'])
        
                for col in part_columns:
                    part_list.append(col['Name'])
        
                col_list_stg += part_list
            else:
                # only get get column list from Glue Catalog
                col_list_stg = []
                columns = response_stg['Table']['StorageDescriptor']['Columns']
                for col in columns:
                    col_list_stg.append(col['Name'])
        # boto3 error handling using ClientError and ParamValidationError errors.
        except ClientError as e:
            print("Glue client returned error: ", e.response['Error']['Message'])
            raise e
        except ParamValidationError as e:
            raise ValueError(f'The parameters you provided are incorrect: {e}')
        
        return col_list_stg
    
    def filter_s3_objs(client, bucket, prefix, tz, t_horizon):
        """ function that creates filtered file object list from S3 bucket using provided time range value
        :param client: S3 client
        :param bucket: S3 bucket
        :param prefix: prefix of the file objects keys used in filtering
        :param tz: local timezone
        :param t_horizon: time in hours used to determine time range filter from current hour
        :return: list of S3 file object keys
        """
        keys_filtered = []
        # get unfiltered object list
        from_hour = int(datetime.now(tz=tz).strftime("%H")) - t_horizon
        try:
            response = client.list_objects(
                Bucket=bucket,
                Prefix=prefix
            )
            objs = response["Contents"]
            # filter objects by LastModified timestamp data
            for ob in objs:
                modified = ob.get("LastModified")
                hour = modified.astimezone(timezone).strftime('%H')
                if int(hour) >= from_hour:
                    name = ob.get("Key")
                    keys_filtered.append(name)
        # boto3 error handling using ClientError and ParamValidationError errors.
        except ClientError as e:
            print("S3 returned error: ", e.response['Error']['Message'])
            raise e
        except ParamValidationError as e:
            raise ValueError(f'The parameters you provided are incorrect: {e}')            
                    
        return keys_filtered
    
    def load_from_s3(s3, bucket, file_keys, cols):
        """ funstion that loads file objects from S3 using provided list of object keys and
        saves them to a single Pandas dataframe
        :param s3: S3 client
        :param bucket: name of S3 bucket
        :param file_keys: list of keys
        :param cols: list of dataframe column names
        :return: Pandas dataframe
        """
        df_list = []
        # iterate over object list and load files to memory
        for file in file_keys:
            try:
                body = s3.get_object(Bucket=bucket, Key=file)["Body"].read().decode('utf-8')
            except ClientError as e:
                print("S3 returned error: ", e.response['Error']['Message'])
                raise e
            except ParamValidationError as e:
                raise ValueError(f'The parameters you provided are incorrect: {e}')    
            # create single object dataframe and append it to the list of dataframes
            df_single = pd.read_csv(StringIO(body), names=cols)
            df_list.append(df_single)
        # create resulting dataframe from the list of dataframes
        return pd.concat(df_list, axis=0, ignore_index=True)
    
    def clean_tweet(string):
        """ function that cleans input string from symbols and abbreviations without verbal meaning, in order to use it
        for text sentiment analysis. It takes input
        :param string: string of the tweet text
        :return: processed text string
        """
        # Using regular expression filtering clean text for url addresses, quotes, non alfanumeric Latin characters
        string = re.sub(r"^(http\S+|ftp|file):\\/\\/[-a-zA-Z0-9+&@#\\/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#\\/%=~_|]", "", str(string), flags=re.MULTILINE)
        string = re.sub(r"\"", "", str(string), flags=re.MULTILINE)
        string = re.sub(r"https\S+", "", str(string), flags=re.MULTILINE)
        string = re.sub(r"RT", "", str(string), flags=re.MULTILINE)
        string = re.sub(r"amp", "", str(string), flags=re.MULTILINE)
        string = re.sub(r"[^\u0000-\uFFFF]", "", str(string), flags=re.MULTILINE)
        string = re.sub(r"([^\w\s]+)", " ", str(string), flags=re.MULTILINE)
        # compile emoji-like symbol pattern
        emoji_pattern = re.compile("["                               
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   "]+", flags=re.UNICODE)
        # filter emoji pattern and line breaks
        string = emoji_pattern.sub(r'', string).replace("\n", "")
        return string
    
    # simple function that cleans hashtags from non-alphanumeric Latin characters
    def clean_hashtags(hashtag):
        hashtag = re.sub(r'([^A-Za-z0-9\s]+)', '', str(hashtag))
        return hashtag
    
    # function that calculates sentiment and polarity of input text using TextBlob library tools
    def text_sentiment(text):
        sentiment = ' '.join(str(s) for s in TextBlob(text).sentiment)
        return sentiment
    
    # function that generates hash key from the input string
    def generate_hash_key(x):
        return base64.b64encode(hashlib.sha1(x).digest())
    
    # next four functions parse timestamp strings and return the required ts format, they are used in the dataframe
    # column mapping (.apply()) operations
    def get_year(x):
        parsed_date = parse(x)
        return parsed_date.year
    
    def get_month(x):
        parsed_date = parse(x)
        return parsed_date.month
    
    def get_day(x):
        parsed_date = parse(x)
        return parsed_date.day
        
    def get_timestamp(date):
        parsed_date = parse(date).strftime('%Y-%m-%d %H:%M:%S')
        return parsed_date
    
    # initiate AWS service clients
    s3 = boto3.resource('s3')
    s3_client = boto3.client("s3")
    g_client = boto3.client('glue')
    
    record_time = datetime.now(tz=timezone).strftime("%Y-%m-%d")

    prefix_string = landing_path + \
                    record_time.split("-")[0] + "/" + \
                    record_time.split("-")[1] + "/" + \
                    record_time.split("-")[2] + "/"
    # get column names from Glue Catalog
    old_cols = get_glue_schema(g_client, account_id, source_db, source_table, partitioned=False)
    new_cols = get_glue_schema(g_client, account_id, target_db, target_table, partitioned=True)
    # get filtered file list from S3 and create dataframe from its files
    files = filter_s3_objs(s3_client, bucket_name, prefix_string, timezone, time_horizon)
    frame = load_from_s3(s3_client, bucket_name, files, old_cols)
    # carry out necessary transformations on the landing data
    frame.set_index('record_id')
    frame['record_id'] = frame['record_id'].astype(str)
    frame['time_stamp'] = frame.timestamp.apply(get_timestamp)
    frame['tweet_id'] = frame['tweet_id'].astype(str)
    frame['text_clean'] = frame.text.apply(clean_tweet)
    frame['sentiment'] = frame.text_clean.apply(text_sentiment)
    _df_sent = frame.sentiment.str.split(" ", expand=True)
    frame['polarity'] = _df_sent[0]
    frame['subjectivity'] = _df_sent[1]
    frame['clean_hashtags'] = frame.hashtags.apply(clean_hashtags)
    frame['hashtag'] = frame.clean_hashtags.str.split(' ')
    _df = frame.explode('hashtag')
    _df.hashtag.replace('', np.nan, inplace=True)
    _df.dropna(subset=['hashtag'], inplace=True)
    _df['hash_string'] = _df['record_id'] + _df['tweet_id']
    # generate PK column
    _df['hash_id'] = _df\
        .hash_string.astype(str).str.encode('UTF-8')\
        .apply(generate_hash_key).str.decode("utf-8")
    # generate partition columns
    _df['year'] = _df.timestamp.apply(get_year)
    _df['month'] = _df.timestamp.apply(get_month)
    _df['day'] = _df.timestamp.apply(get_day)

    df = _df[new_cols].copy()
    count_row = df.shape[0]
    # save final dataframe as csv to memory buffer
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, header=False, index=False)
    log_record = ""
    
    try:
        # save csv file to S3 bucket
        s3.Object(bucket_name, staging_path+staging_file+'_'+record_time+'.csv').put(Body=csv_buffer.getvalue())
        print(f"{count_row} records saved to Staging")
        log_record = f"'{record_time}', '{target_db}.{target_table}', {count_row}, {get_year(record_time)}, {get_month(record_time)}, {get_day(record_time)}"
    # boto3 error handling using ClientError and ParamValidationError errors.
    except ClientError as e:
        print("S3 put Object returned error: ", e.response['Error']['Message'])
        raise e
    except ParamValidationError as e:
        raise ValueError(f'The parameters you provided are incorrect: {e}')
    # return indicators of data processing to be used by update-data-log Lambda.
    return log_record
