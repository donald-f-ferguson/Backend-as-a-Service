item1 ={
        'product_id': 'od1',
        'kind': 'book',
        'title': 'Database Manager Systems',
        'isbn': "978-0072465631",
        'authors': [
            {
                'last_name': 'Gherke',
                'first_name': 'Johannes'
            },
            {
                'last_name': 'Ramakrishnan',
                'first_name': 'Raghu'
            }
        ],
       'edition': '3rd',
       'categories': ['books', 'software', 'databases']
    }

item2 = {
        'product_id': 'molm',
        'kind': 'Movie',
        'formats': ['online', 'dvd', 'vhs'],
        'title': 'Man of La Mancha',
        'artists': [
            {
                "directors": [
                    {
                        'last_name': 'Hiller',
                        'first_name': 'Arther'
                    }
                ],
                "actors": [
                    {
                        'last_name': "O'Toole",
                        'first_name': 'Peter'
                    },
                    {
                        'last_name': "Loren",
                        'first_name': 'Sophia'
                    }
                ]
            }
        ],
        'genres': ['musical', 'broadway', 'culture'],
        'running time': 128,
        'languages': ['english']
}

import boto3
import json
import dynamodb_json
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('products')

table.put_item(
    Item=item1
)
table.put_item(
    Item=item2
)







