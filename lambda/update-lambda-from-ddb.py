""" Lambda function that handles updating of environmental variables of all other Lambda functions in the project.
Environmental variable data is stored on DynamoDB. When it is updated this Lambda is triggered by DynamoDB Streams.
DynamoDB Stream provides keys for updated Lambda functions.  This Lambda parses stream data, generates a list of updatable
Lambda functions, then retrieves their environmental variable data from DynamoDB and performs updates
in respective Lambda functions. It requires environmental variables
:param MY_AWS_REGION, AWS region where DynamoDB database has been created.
:param DDB_TABLE, the name of DynamoDB table.
:param DDB_KEY, the name of defined primary key in DynamoDB table.
"""
import json
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import os


aws_region = os.environ['MY_AWS_REGION']
table_name = os.environ['DDB_TABLE']
key = os.environ['DDB_KEY']

dynamodb_client = boto3.client('dynamodb', region_name=aws_region)
dynamodb_resource = boto3.resource('dynamodb', region_name=aws_region)
client = boto3.client('lambda')


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
    def get_ddb_item(tbl_name, region, key_name, item_name, ddb_r=None):
        """ function that loads loads item data from DynamoDB. Environmental variable data is located under
         'Variables' key, therefore it is used to retrieve the relevant data. It requires input
        :param tbl_name: the name of DynamoDB table
        :param region: the name of AWS region where DynomoDB table is located
        :param key_name: the name of primary key in DynamoDB table
        :param item_name: name of the item to retrieve the data
        :param ddb_r: DynamoDB resource client name
        :return: json object with environmental variable data
        """
        if not ddb_r:
            ddb_r = boto3.resource('dynamodb', region_name=region)
        # instantiate table object
        table = ddb_r.Table(tbl_name)
        # get item data from DynamoDB and parse data under 'Variables' key into json object
        response = table.get_item(Key={key_name: item_name})
        env = response['Item']['Variables'].replace("'", "\"")
        env_dict = json.loads(env)
        return env_dict

    @boto_safe_run
    def update_lambda(name, params, l_client=None):
        """ function that updates environmental variables for Lambda function by function name. It tkaes input
        :param name: name of the LAmbda function to be updated
        :param params: json object containing environmental variable data
        :param l_client: Lambda service client
        :return: string with information about the performed update
        """
        if not l_client:
            l_client = boto3.client('lambda')
        # perform update
        result = l_client.update_function_configuration(
            FunctionName=name,
            Environment={"Variables": params})
        # handle update response
        response = f"Updated lambda {name}, environmental variables: ", json.dumps(result['Environment'])
        print(f"Updated lambda {name}, environmental variables: ", json.dumps(result['Environment']))
        if 'Error' in result['Environment']:
            response = "Update error: " + result['Environment']['Error']['Message']
    
        return response

    lambda_list = []

    # iterate over DynamoDB Stream data and store Lambda function names that have been updated into list
    for record in event['Records']:
        print("Event type: ", record['eventName'])
        lambda_key = record["dynamodb"]["Keys"]["Lambda"]["S"]
        lambda_list.append(lambda_key)

    # iterate over list of Lambda function names and perform updates
    for lambda_id in lambda_list:
        print(lambda_id)
        
        settings = get_ddb_item(table_name, aws_region,  key, lambda_id, dynamodb_resource)
        update = update_lambda(lambda_id, settings, client)
        print(update)
    
    return json.dumps({"exit_status":"SUCCESS"})
