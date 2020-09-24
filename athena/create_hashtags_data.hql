CREATE EXTERNAL TABLE IF NOT EXISTS `analytical.hashtag_data`(
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
  `text_clean` string)
PARTITIONED BY ( 
  `year` int, 
  `month` int, 
  `day` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://tweet-etl/analytical/hashtag_data'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'last_modified_by'='hadoop', 
  'last_modified_time'='1599224978')