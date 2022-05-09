from datetime import datetime
import boto3
import os
import uuid
import json
import logging
import dynamo  # utility function to transform dict object to dynamodb object, vice verca
import uuid


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set up access to dynamodb
dynamodb = boto3.client('dynamodb')


# another way of doing it
# dynamodb = boto3.resource(
#     'dynamodb', region_name=str(os.environ['REGION_NAME']))


# get the table name from environment and check it for validity
table_name = str(os.environ['DYNAMODB_TABLE'])


def create(event, context):
    """this creates a log into dynamodb with event to be loaded """
    logger.info(f'Incoming request is: {event}')

    # Set the default error response, this will change otherwise
    response = {
        "statusCode": 500,
        "body": "An error occured while creating post."
    }
    
    # extract the body out of the event
    post_str = event['body'] 
    
    # transform the json response to python dict
    post = json.loads(post_str)
    # convert the current time to isoformat
    current_timestamp = datetime.now().isoformat()

    post['createdAt'] = current_timestamp
    post['id'] = str(uuid.uuid4())
    


    res = dynamodb.put_item(
        TableName=table_name,
        Item=dynamo.to_item(post)
    )

    # If creation is successful
    if res['ResponseMetadata']['HTTPStatusCode'] == 200:
        response = {
            "statusCode": 201,
        }

    return response


def get(event, context):
    """This function fetches entries based on postId in the dynamo db"""
    print(":::::==>>>", event['pathParameters'])
    logger.info(f'Incoming request is: {event}')
    # Set the default error response
    response = {
        "statusCode": 500,
        "body": "An error occured while getting post."
    }
    print(":::::==>>>", event) # debugging 

    post_id = event['pathParameters']['postId']

    post_query = dynamodb.get_item(
        TableName=table_name, Key={'id': {'S': post_id}})
    
    if 'Item' in post_query:
        post = post_query['Item']
        logger.info(f'Post is: {post}')
        response = {
            "statusCode": 200,
            'headers': {'Content-Type': 'application/json'},
            "body": json.dumps(dynamo.to_dict(post)) #  converts  to json
        }

    return response


def all(event, context):
    """This fetches all enteries from the dynamodb and returns it """
    # Set the default error response
    response = {
        "statusCode": 500,
        "body": "An error occured while getting all posts."
    }
    

    scan_result = dynamodb.scan(TableName=table_name)['Items'] # only works if the size of table is less than or equal to  1mb
    #need to use  LastEvaluatedKey use code below for large loads
    """
    response = dynamodb.scan(TableName=table_name)
    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        scan_result.extend(response['Items'])
    """

    posts = []

    for item in scan_result:
        posts.append(dynamo.to_dict(item))

    response = {
        "statusCode": 200,
        "body": json.dumps(posts)
    }

    return response


def update(event, context):
    """Update entries in dynamo db"""
    logger.info(f'Incoming request is: {event}')

    post_id = event['pathParameters']['postId'] #pathParameters gives you the postId

    # Set the default error response
    response = {
        "statusCode": 500,
        "body": f"An error occured while updating post {post_id}"
    }

    post_str = event['body'] # the event body gives you the message sent

    post = json.loads(post_str) # convert to dict

    res = dynamodb.update_item(
        TableName=table_name,
        Key={
            'id': {'S': post_id}
        },
        UpdateExpression="set content=:c, author=:a, updatedAt=:u", # expression keys
        ExpressionAttributeValues={
            ':c': dynamo.to_item(post['content']), # convert to dynamodb object before updating
            ':a': dynamo.to_item(post['author']),
            ':u': dynamo.to_item(datetime.now().isoformat())

        },
        ReturnValues="UPDATED_NEW"
    )

    # If updation is successful for post
    if res['ResponseMetadata']['HTTPStatusCode'] == 200:
        response = {
            "statusCode": 200,
        }

    return response


def delete(event, context):
    """Delete entries in dynamodb"""
    logger.info(f'Incoming request is: {event}')

    post_id = event['pathParameters']['postId']

    # Set the default error response
    response = {
        "statusCode": 500,
        "body": f"An error occured while deleting post {post_id}"
    }

    res = dynamodb.delete_item(TableName=table_name, Key={
                               'id': {'S': post_id}})

    # If deletion is successful for post
    if res['ResponseMetadata']['HTTPStatusCode'] == 200:
        response = {
            "statusCode": 204,
        }
    return response
