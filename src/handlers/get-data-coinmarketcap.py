import boto3
import json
from datetime import datetime, timedelta


def coinmarketcap_get_data(event, context):

    # ALLOW ORIGIN
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }

    # QUERY DYNAMO 

    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Specify the table name
    table_name = 'coinmarketcap'

    # Define the desired date for querying (in UTC)
    desired_date = datetime(2023, 5, 15)

    # Calculate the start and end timestamps for the desired day
    start_timestamp = desired_date
    end_timestamp = desired_date + timedelta(days=1)

    # Convert the timestamps to ISO 8601 format
    start_timestamp_iso = start_timestamp.strftime('%Y-%m-%dT%H:%M:%S')
    end_timestamp_iso = end_timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    # Define the FilterExpression
    filter_expression = '#date between :start_date and :end_date'

    # Define the ExpressionAttributeValues
    expression_attribute_values = {
        ':start_date': {'S': '2023-05-14-23:59:19'},
        ':end_date': {'S': '2023-05-15-04:47:19'}
    }


    # Define the ExpressionAttributeNames
    expression_attribute_names = {
        '#date': 'date'
    }

    # Scan the table with the filter expression
    myQuery = dynamodb.scan(
        TableName=table_name,
        FilterExpression=filter_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names
    )


    response = {
        'statusCode': 200,
        'body': json.dumps(myQuery),
        'headers' : headers
    }
    print('********RESPONSE*********')
    return response
    