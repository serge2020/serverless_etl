# Serverless ETL Solution on AWS Cloud

This project intends to demonstrate on a POC (proof-of-concept) level what it takes to create scalable, serverless and automated ETL solution on AWS Cloud.
It doesn't have a goal to meet all requirements of production-grade application (for now), never the less this solution includes all typical stages of the data pipeline: from source data loading up to the dashboard visualistations.
From the data subject perspective the solution analyses popularity of recent Twitter hashtags and text sentiment of the tweets in which they appears well as the users who have used them.   
## Table of Contents
  * [Solution Architecture](#solution-architecture)
      - [Data processing](#data-processing)
      - [Scheduling, monitoring & configuration](#scheduling--monitoring---configuration)
  * [TOP Hashtags dashboard](#top-hashtags-dashboard)
  * [Repo structure](#repo-structure)
  * [License](#license)




## Solution Architecture
The overall approach for development stage was as much as possible to stay within AWS Free Tier. 
Only serverless AWS services are used to build the project architecture that is comprised from two logical activity layers: 
  1. Data processing activities
  2. Pipeline configuration, scheduling & monitoring activities
  
[Twitter Streaming API](https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data) is used as the data source and since AWS Quicksight dashboards embedding is supported only in Enterprise Edition, Power BI dashboard is used for data visualisation.  
Check out this dashboard with the latest hashtag data or proceed with solution architecture details!
#### Data processing

[AWS Lambda](https://aws.amazon.com/lambda/) functions are used to carry out all data processing stages and deploy the relevant AWS services at each stage:
* Data loading from source - [AWS Kinesis](https://aws.amazon.com/kinesis/)
* Data storage - [AWS S3](https://aws.amazon.com/s3/), four database layers are used in line with ETL tasks:
  1. **Landing**: stores initial streaming data loaded from Twitter API in csv format
  2. **Staging**: batch processing of data from the Landing layer, old data is removed
  3. **Analytical**: processed data is inserted from AWS Athena table  
    
* Data analytics - [AWS Athena](https://aws.amazon.com/athena/) and [AWS Quicksight](https://aws.amazon.com/quicksight/)

#### Scheduling, monitoring & configuration
The following AWS services are used for automated data processing:
* **Lambda function orchestration** - [AWS Step Functions](https://aws.amazon.com/step-functions/). Two State Machines manage the workflow. First one handles Twitter data loading to S3 Landing layer while the second one manages data transformations and loading to Analytical layer.
* **Scheduling** - [AWS EventBridge](https://aws.amazon.com/eventbridge/) schedules the runs of the State Machines
* **Lambda function configuration** - environmental variable data is stored in [DynamoDB](https://aws.amazon.com/dynamodb/) table. Each data update goes into DynamoDB Streams that trigger Lambda function responsible for updating environmental variables to their current values.
* **Athena table schema information** - [AWS Glue](https://aws.amazon.com/glue/) Data Catalog
* **Error notifications** - [AWS CloudWatch](https://aws.amazon.com/cloudwatch/) Alert is triggered when State Machine run fails which in turn triggers [AWS SNS](https://aws.amazon.com/sns/) to send notification email

## TOP Hashtags dashboard
dashboard with the latest hashtag data

## Repo structure

The folders below contain scripts that implement core functionality of this solution:
* **athena** Hive scripts used to create Athena tables
* **lambda** Python code of the Lambda functions used in the project
* **step_functions** JSON files with definitions of Step Function state machines. Their workflow graphs are shown below:
  * Tweet data loading to S3 (KinesisLandingStateMachine)
  
  ![KinesisLandingStateMachine]()
  * Data processing and saving to Analytical layer and Quicksight (LandingAnalyticalStateMachine)
  
  ![LandingAnalyticalStateMachine]()

## License
[MIT](https://choosealicense.com/licenses/mit/)