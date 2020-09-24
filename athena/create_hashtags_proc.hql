CREATE EXTERNAL TABLE IF NOT EXISTS `staging.hashtags_proc`(
  `hash_id` string, 
  `record_id` string, 
  `time_stamp` timestamp, 
  `created` timestamp, 
  `tweet_id` string, 
  `user_name` string, 
  `rt_count` int, 
  `hashtag` string, 
  `polarity` float, 
  `subjectivity` float, 
  `text` string, 
  `year` int, 
  `month` int, 
  `day` int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://tweet-etl/staging/hashtags_proc'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'transient_lastDdlTime'='1599133210')