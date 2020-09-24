""" Lambda function that creates Kinesis producer which sends tweet data to Kinesis Data stream
using MyStreamListener class from stream_listener.py. It requires environmental variables
:param MY_AWS_REGION, AWS region where Kinesis stream has been created.
:param TWITTER_API_ENDPOINT, Twitter streaming API endpoint URL (https://api.twitter.com/labs/1/tweets/stream/sample)
:param STREAM_NAME, the name of Kinesis data stream
:param STREAM_SECONDS, activity time in seconds for Kinesis producer
"""
import json
import boto3
import os
import tweepy
import stream_listener

aws_region = os.environ['MY_AWS_REGION']
url = os.environ['TWITTER_API_ENDPOINT']
stream_name = os.environ['STREAM_NAME']
time_limit_cnf = int(os.environ['STREAM_SECONDS'])


def lambda_handler(event, context):
    
    k_client = boto3.client('kinesis')
    # load Twitter credentials from AWS Parameter Store
    ssm = boto3.client("ssm", region_name=aws_region)
    tokens = ssm.get_parameter(Name="/data-stream/twitter/token", WithDecryption=True)['Parameter']['Value']
    tokens_list = tokens.split(",")
    key = tokens_list[0]
    key_secret = tokens_list[1]
    token = tokens_list[2]
    token_secret = tokens_list[3]
    # Connect to Twitter API and run Kinesis producer
    auth = tweepy.OAuthHandler(key, key_secret)
    auth.set_access_token(token, token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    stream = stream_listener.MyStreamListener(api, time_limit_cnf, k_client, stream_name)
    tweet_stream = tweepy.Stream(auth=api.auth, listener=stream, tweet_mode='extended')
    tweet_stream.sample()
    
    return json.dumps({"exit_status":"SUCCESS"})