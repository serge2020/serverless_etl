""" Lambda function that inserts records with new data processing indicators into operational.data_update_log table.
Function also return indicators of data processing to be used by update-data-log Lambda. It requires environmental variables
:param OPERATIONAL_DB, the name of target (Operational) database in Glue Catalog
:param TARGET_TABLE, the name of target (Analytical) table in Glue Catalog
:param ATHENA_OUT, output directory path, required parameter for Athena query execution
"""
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import os

database = os.environ['OPERATIONAL_DB']
output = os.environ['ATHENA_OUT']
target_table = os.environ['TARGET_TABLE']


def lambda_handler(event, context):
    # parse event object supplied by Step Functions trigger into Athen query
    insert_values = event["values"]
    query = f'INSERT INTO {target_table} VALUES ({insert_values})'
    
    client = boto3.client('athena')

    try:
        # insert record of data processing indicators into operational.data_update_log table
        response_1 = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': output
            }
        )
        execution_id = response_1['QueryExecutionId']
        status = 'RUNNING'
        while (status == 'QUEUED') or (status == 'RUNNING'):
            # get current query execution status
            response_2 = client.get_query_execution(QueryExecutionId=execution_id)
            status = response_2['QueryExecution']['Status']['State']

        print('Query result: ', status)
        if status != 'SUCCEEDED':
            # handle query execution errors
            print('Error: ', response_2['QueryExecution']['Status']['StateChangeReason'])
    # boto3 error handling using ClientError and ParamValidationError errors.
    except ClientError as e:
        print("Glue client returned error: ", e.response['Error']['Message'])
        raise e
    except ParamValidationError as e:
        raise ValueError(f'The parameters you provided are incorrect: {e}')
    return status
