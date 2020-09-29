""" Lambda function that loads the data from Staging layer Athena table and inserts it into analytical.hashtag_data table.
Function also return indicators of data processing to be used by update-data-log Lambda. It requires environmental variables
:param TARGET_DB, the name of target (Analytical) database in Glue Catalog
:param TARGET_TABLE, the name of target (Analytical) table in Glue Catalog
:param SOURCE_TABLE, the name of source (Staging) table in Glue Catalog
:param ATHENA_OUT, output directory path, required parameter for Athena query execution
:param TIME_ZONE, UTC timezone abbreviation to be used as home timezone, eg. 'Europe/Helsinki'
"""
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import os
from datetime import datetime
from dateutil.parser import *
import pytz


target_db = os.environ['TARGET_DB']
output = os.environ['ATHENA_OUT']
source_tbl = os.environ['SOURCE_TABLE']
target_tbl = os.environ['TARGET_TABLE']
timezone = pytz.timezone('Europe/Helsinki')

# Assigning Athena query variables
query_1 = f"SELECT COUNT(time_stamp) AS count FROM {source_tbl}"
query_2 = f"INSERT INTO {target_tbl} \
    (SELECT * FROM {source_tbl})"
rows_inserted = "0"


def lambda_handler(event, context):
    def boto_safe_run(func):
        """ decorator (higher order function) to handle errors using boto3 using ClientError and ParamValidationError error types
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
    
    @boto_safe_run
    def run_athena_query(client, query_string, db_name, output_path):
        """ function that handles Athena query execution. It does not finish until query status indicates that
        it has finished. It takes input
        :param client: Athena client
        :param query_string: Athena query string
        :param db_name: Athena database name
        :param output_path: Athena output path
        :return: list of finished query execution indicators (query status, error message, execution id)
        """
        # start query execution
        response_1 = client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': db_name
            },
            ResultConfiguration={
                'OutputLocation': output_path
            }
        )
        # get query execution id from execution response object
        execution_id = response_1['QueryExecutionId']
        status = 'RUNNING'
        errors = "None"
        while (status == 'QUEUED') or (status == 'RUNNING'):
            # get current query execution status
            response_2 = client.get_query_execution(QueryExecutionId=execution_id)
            status = response_2['QueryExecution']['Status']['State']

        if status != 'SUCCEEDED':
            # handle query execution errors
            status = response_2['QueryExecution']['Status']['State']
            errors = response_2['QueryExecution']['Status']['StateChangeReason']
            print("Athena query status: ", status, ", errors: ", errors)
            raise ClientError
        return [status, errors, execution_id]

    @boto_safe_run
    def get_athena_result(client, id, max_results):
        """ function that implements get_query_results() call to retrieve results of successfully executed Athena query.
         It requires input
        :param client: Athena service client
        :param id: get_query_results() QueryExecutionId parameter
        :param max_results: get_query_results() MaxResults parameter, max number of rows to return
        :return: resulting data object of query execution
        """
        data = []
        response = client.get_query_results(QueryExecutionId=id, MaxResults=max_results)
        data = response['ResultSet']['Rows']
        return data

    # next three functions parse timestamp strings and return the required ts format, they are used to compile the output
    # record this Lambda function returns
    def get_year(x):
        parsed_date = parse(x)
        return parsed_date.year
    
    def get_month(x):
        parsed_date = parse(x)
        return parsed_date.month
    
    def get_day(x):
        parsed_date = parse(x)
        return parsed_date.day
        
    client = boto3.client('athena')
    # get current count of rows in staging table
    result_1 = run_athena_query(client, query_1, target_db, output)
    result_2 = get_athena_result(client, result_1[2], 3)
    row_count = result_2[1]["Data"][0]["VarCharValue"]
    
    # if there is data in the staging table insert it into analytical table
    if int(row_count) > 0:
        result_3 = run_athena_query(client, query_2, target_db, output)
        print("Query status: ", result_3[0], ", errors: ", result_3[1])
        if result_3[1] == "None":
            rows_inserted = row_count
    
    print("rows_inserted: ", rows_inserted)

    record_time = datetime.now(tz=timezone).strftime("%Y-%m-%d %H:%M:%S")
    out_record = f"'{record_time}', '{target_tbl}', {rows_inserted}, {get_year(record_time)}, {get_month(record_time)}, {get_day(record_time)}"
    # return indicators of data processing to be used by update-data-log Lambda.
    return out_record