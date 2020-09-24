""" Python script that creates MyStreamListener class used by kinesis-producer-send-tweets lambda function. """
import time
import tweepy
import json
from botocore.exceptions import ClientError, ParamValidationError

class MyStreamListener(tweepy.StreamListener):
    """
    Class MyStreamListener extends StreamListener parent class provided by Tweepy library. It is used to handle
    data stream from Twitter streaming API and put filtered records into Kinesis Data Stream shard.
    """
    def __init__(self, api, time_limit, kinesis_client, stream_name):
        """ Class constructor, defines class parameters
        :param api: Twitter API authorised connection object
        :param time_limit: time to run in seconds
        :param kinesis_client: Kinesis service client
        :param stream_name: name of Kinesis stream
        """
        self.api = api
        self.me = api.me()
        self.start_time = time.time()
        self.limit = time_limit
        self.kinesis_client = kinesis_client
        self.stream_name = stream_name
        super().__init__()

    def on_status(self, status):
        """ Function that performs multi step filtering of the tweet stream. It takes value of tweet record
        :param status: "status" field as input value. Then it filters tweet records based on:
        1. If they are retweets
        2. If tweet language is English
        3. If tweet has been retweeted at least 100 times
        Six relevant fields of the Filtered tweets are stored into dictionary and then saved to Kinesis stream
        :return: Boolean True while the time limit hasn't been reached, then False
        """

        if (time.time() - self.start_time) < self.limit:
            record = []
            tweet_txt = ""
            # filter tweet records
            if hasattr(status, 'retweeted_status') and status.retweeted_status.lang == "en":
                rs = status.retweeted_status
                if hasattr(rs, "extended_tweet"):
                    if hasattr(rs.extended_tweet, "full_text") and rs.extended_tweet.full_text != "":
                        tweet_txt = rs.extended_tweet.full_text
                else:
                    tweet_txt = rs.text

                created = str(rs.created_at)
                tweet_id = rs.id_str
                user_name = rs.user.screen_name
                rt_count = rs.retweet_count
                text = tweet_txt
                tweet_dict = {}
                tweet_dict = rs.entities
                hashtags = tweet_dict["hashtags"]
                record_dict = {}
                if hashtags and rt_count > 99 and text != "":
                    ht = [x["text"] for x in hashtags]
                    ht = ' '.join(ht)
                    # save tweet data to Kinesis stream
                    record_dict = {"created": created,
                                   "tweet_id": tweet_id,
                                   "user_name": user_name,
                                   "rt_count": rt_count,
                                   "hashtags": ht,
                                   "text": text
                                   }
                    _record = json.dumps(record_dict)

                    record_encoded = bytes(str(_record), 'utf-8')

                    try:
                        response = self.kinesis_client.put_record(Data=record_encoded,
                                                                  StreamName=self.stream_name,
                                                                  PartitionKey='111')
                    # boto3 error handling using ClientError and ParamValidationError errors.
                    except ClientError as e:
                        print("Kinesis stream returned error: ", e.response['Error']['Message'])
                        raise e
                    except ParamValidationError as e:
                        raise ValueError(f'The parameters you provided are incorrect: {e}')

            return True
        else:
            return False
