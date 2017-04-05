"""
    Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
    This program is licensed to you under the Apache License Version 2.0,
    and you may not use this file except in compliance with the Apache License
    Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
    http://www.apache.org/licenses/LICENSE-2.0.
    Unless required by applicable law or agreed to in writing,
    software distributed under the Apache License Version 2.0 is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
    express or implied. See the Apache License Version 2.0 for the specific
    language governing permissions and limitations there under.
"""


DYNAMODB_RUNID_ATTRIBUTE = 'RunId'


def add_to_manifest(dynamodb_client, table_name, run_id):
    """Add run_id into DynamoDB manifest table

    Arguments:
    dynamodb_client - boto3 DynamoDB client (not service)
    table_name - string representing existing table name
    run_id - string representing run_id to store
    """
    dynamodb_client.put_item(
        TableName=table_name,
        Item={
            DYNAMODB_RUNID_ATTRIBUTE: {
                'S': run_id
            }
        }
    )


def is_in_manifest(dynamodb_client, table_name, run_id):
    """Check if run_id is stored in DynamoDB table.
    Return True if run_id is stored or False otherwise.

    Arguments:
    dynamodb_client - boto3 DynamoDB client (not service)
    table_name - string representing existing table name
    run_id - string representing run_id to store
    """
    response = dynamodb_client.get_item(
        TableName=table_name,
        Key={
            DYNAMODB_RUNID_ATTRIBUTE: {
                'S': run_id
            }
        }
    )
    return response.get('Item') is not None
