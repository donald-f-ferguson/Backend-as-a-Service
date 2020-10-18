import pandas as pd
import pymysql
import logging

import boto3
import json
import decimal

from boto3.dynamodb.conditions import Key, Attr
from src.data_tables.BaseDataTable import BaseDataTable

logger = logging.getLogger()


dynamodb = boto3.resource('dynamodb', region_name='us-east-1') #, endpoint_url="http://localhost:8000")



class DynamoDBDataTable(BaseDataTable):


    def __init__(self, table_name, key_columns=None, connect_info=None):
        """

        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
            This is for compatibility with other types of data table. Any value other than None is an error.
        """

        # If there is not explicit connect information, use the defaults.
        self._table_name = table_name
        self._db_table  = dynamodb.Table('W4111GoTEpisodes')
        self._key_schema = self._db_table.key_schema
        self._key_fields = self._get_key_fields()

    def __str__(self):
        """

        :return: String representation of the table's metadata.
        """
        result = "DynamoDBDataTable: Name=" + self._table_name
        result += "\n\tKey Schema: " + json.dumps(self._key_schema)
        result += "\n\tKey Fields: " + json.dumps(self._key_fields)

        return result

    def _get_key_fields(self):

        fields = []

        for kt in self._key_schema:
            if kt["KeyType"] == "HASH":
                fields.append(kt["AttributeName"])

        fields.sort()

        return fields

    def _run_q(self, q, args=None, fields=None, fetch=True, cnx=None, cursor=None, commit=True):
        """

        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param cncursor: Do not worry about this for now.
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """
        pass

    def _run_insert(self, table_name, column_list, values_list, cnx=None, commit=True):
        """

        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Ignore this for now.
        :param commit: Ignore this for now.
        :return:
        """
        pass

    def get_key_from_values(self, key_fields):

        key={
        }
        for i in range(0,len(self._key_fields)):
            key[self._key_fields[i]] = key_fields[i]

        return key


    def get_folders(self):
        pass

    def find_by_primary_key(self, key_fields, field_list=None, context=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """
        key = self.get_key_from_values(key_fields)
        result = self._db_table.get_item(Key=key)
        result = result.get('Item')
        return result

    def _template_to_where_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """

        fe = None

        if t is not None:
            for k,v in t.items():
                if fe is None:
                    fe = Attr(k).eq(v)
                else:
                    fe = fe and Attr(k).eq(v)

        return fe


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        fe = self._template_to_where_clause(template)
        result = self._db_table.scan(FilterExpression=fe)
        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        pass

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        pass

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        pass

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        pass

    def load(self):
        pass

    def save(self):
        pass

    def query(self, query_statement, args, context=None):
        pass

