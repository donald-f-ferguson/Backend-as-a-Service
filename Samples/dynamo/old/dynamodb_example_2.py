import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('products')

response = table.get_item(
        Key={
            'product_id': 'molm'
        }
    )
print(response)
response = response['Item']

response['running time'] = \
    str(response['running time'])

print('Response without Decimal = ',
      json.dumps(response, indent=2))





