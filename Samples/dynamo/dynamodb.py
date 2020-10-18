import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
from datetime import datetime
import time
import uuid

# There is some weird stuff in DynamoDB JSON responses. These utils work better.
# I am not using  in this example.
# from dynamodb_json import json_util as jsond

# There are a couple of types of client.
# I create both because I like operations from both of them.
#
# I comment out the key information because I am getting this from
# my ~/.aws/credentials files. Normally this comes from a secrets vault
# or the environment.
#
dynamodb = boto3.resource('dynamodb',
                          # aws_access_key_id=aws_access_key_id,
                          # aws_secret_access_key=aws_secret_access_key,
                          region_name='us-east-1')

other_client = boto3.client("dynamodb")


def get_item(table_name, key_value):
    table = dynamodb.Table(table_name)

    response = table.get_item(
        Key=key_value
    )

    response = response.get('Item', None)
    return response


def do_a_scan(table_name, filterexpression):
    table = dynamodb.Table(table_name)

    if filterexpression is not None:
        print("Scan with expression")
        if filterexpression is not None:
            response = table.scan(
                FilterExpression=filterexpression
            )
        else:
            response = table.scan()
    else:
        response = table.scan(
        )

    print("Scan succeeded")
    #print(json.dumps(response, indent=4))
    return response["Items"]


def put_item(table_name, item):

    table = dynamodb.Table(table_name)
    res = table.put_item(Item=item)
    return res


def add_response(table_name, comment_id, commenter_email, response):
    table = dynamodb.Table(table_name)
    Key={
        "comment_id": comment_id
    }
    dt = time.time()
    dts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(dt))

    full_rsp = {
        "email": commenter_email,
        "datetime": dts,
        "response": response,
        "response_id": str(uuid.uuid4()),
        "version_id": str(uuid.uuid4())
    }
    UpdateExpression="SET responses = list_append(responses, :i)"
    ExpressionAttributeValues={
        ':i': [full_rsp]
    }
    ReturnValues="UPDATED_NEW"

    res = table.update_item(
        Key=Key,
        UpdateExpression=UpdateExpression,
        ExpressionAttributeValues=ExpressionAttributeValues,
        ReturnValues=ReturnValues
    )

    return res


def find_by_template(table_name, template):


    fe = ' AND '.join(['{0}=:{0}'.format(k) for k, v in template.items()])
    ea = {':{}'.format(k):v for k, v in template.items()}

    tbl = dynamodb.Table(table_name)
    result = tbl.scan(
        FilterExpression=fe,
        ExpressionAttributeValues=ea
    )
    return result


def add_comment(email, comment, tags):
    dt = time.time()
    dts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(dt))

    item = {
        "comment_id": str(uuid.uuid4()),
        "version_id": str(uuid.uuid4()),
        "email": email,
        "comment": comment,
        "tags": tags,
        "datetime": dts,
        "responses": []
    }

    res = put_item("comments", item=item)

    return res




