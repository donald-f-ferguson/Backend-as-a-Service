import boto3
import json
import decimal
import os

#aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
#aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

dynamodb = boto3.resource("dynamodb",
                        #aws_access_key_id=aws_access_key_id,
                        #aws_secret_access_key=aws_secret_access_key,
                        region_name='us-east-1')

import boto3

"""
dynamodb = boto3.client('dynamodb',
                      # aws_access_key_id=aws_access_key_id,
                      # aws_secret_access_key=aws_secret_access_key,
                      region_name='us-east-1')
"""

from boto3.dynamodb.conditions import Key, Attr


def t1():
    tbls = dynamodb.list_tables()
    print("Tables = ", json.dumps(tbls["TableNames"], indent=2))

"""
def t2():

    table = dynamodb.Table('CustomerProfile')
    response = table.get_item(
        Key={
            'customer_id': "dofe1"
        }
    )
    print("Item = ", json.dumps(response["Item"], indent=2, default=str))
"""

def t3():
    client2 = boto3.client('dynamodb')
    table = client2.describe_table(TableName='comments')
    print("INFO = ", json.dumps(table, indent=2, default=str))

def t4():
    pass

#t1()

#t2()

#t3()

