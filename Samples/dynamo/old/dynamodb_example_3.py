import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('products')

ean = { "#yr": "year", }
fe = Key('year').between(1950, 1959)
response = table.query(
    KeyConditionExpression=Key('kind').eq("movie")
)

for i in response['Items']:
    print(i)





