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

import sys

from datetime import datetime
from botocore.exceptions import ClientError

DYNAMODB_RUNID_ATTRIBUTE = 'RunId'


class RunManifests(object):
    """Wrapper class"""
    def __init__(self, dynamodb_client, table_name):
        self.dynamodb = dynamodb_client
        self.table_name = table_name

    def create(self):
        return create_manifest_table(self.dynamodb, self.table_name)

    def add(self, run_id):
        return add_to_manifest(self.dynamodb, self.table_name, run_id)

    def contains(self, run_id):
        return is_in_manifest(self.dynamodb, self.table_name, run_id)


def create_manifest_table(dynamodb_client, table_name):
    """Create DynamoDB table for run manifests

    Arguments:
    dynamodb_client - boto3 DynamoDB client (not service)
    table_name - string representing existing table name
    """
    try:
        dynamodb_client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': DYNAMODB_RUNID_ATTRIBUTE,
                    'AttributeType': 'S'
                },
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': DYNAMODB_RUNID_ATTRIBUTE,
                    'KeyType': 'HASH'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        dynamodb_client.get_waiter('table_exists').wait(TableName=table_name)
    except ClientError as e:
        # Table already exists
        if e.response['Error']['Code'] == 'ResourceInUseException':
            pass
        else:
            raise e


def list_runids(s3_client, full_path):
    """Return list of all run ids inside S3 folder. It does not respect
    S3 pagination (`MaxKeys`) and returns **all** keys from bucket

    Arguments:
    s3_client - boto3 S3 client (not service)
    full_path - full valid S3 path to events (such as enriched-archive)
                example: s3://acme-events-bucket/main-pipeline/enriched-archive
    """
    listing_finished = False                 # last response was not truncated
    run_ids_buffer = []
    last_continuation_token = None

    (bucket, prefix) = split_full_path(full_path)

    while not listing_finished:
        options = clean_dict({
            'Bucket': bucket,
            'Prefix': prefix,
            'Delimiter': '/',
            'ContinuationToken': last_continuation_token
        })

        response = s3_client.list_objects_v2(**options)
        keys = [extract_run_id(key['Prefix']) for key in response.get('CommonPrefixes', [])]
        run_ids_buffer.extend([key for key in keys if key is not None])
        last_continuation_token = response.get('NextContinuationToken', None)

        if not response['IsTruncated']:
            listing_finished = True

    return run_ids_buffer


def split_full_path(path):
    """Return pair of bucket without protocol and path

    Arguments:
    path - valid S3 path, such as s3://somebucket/events

    >>> split_full_path('s3://mybucket/path-to-events')
    ('mybucket', 'path-to-events/')
    >>> split_full_path('s3://mybucket')
    ('mybucket', None)
    """
    if path.startswith('s3://'):
        path = path.lstrip('s3://')
    elif path.startswith('s3n://'):
        path = path.lstrip('s3n://')
    else:
        raise ValueError("S3 path should start with s3:// or s3n:// prefix")
    parts = path.split('/')
    bucket = parts[0]
    path = '/'.join(parts[1:])
    return bucket, normalize_prefix(path)


def extract_run_id(key):
    """Extract date part from run id

    Arguments:
    key - full key name, such as shredded-archive/run=2012-12-11-01-31-33/
          (trailing slash is required)

    >>> extract_run_id('shredded-archive/run=2012-12-11-01-11-33/')
    'shredded-archive/run=2012-12-11-01-11-33/'
    >>> extract_run_id('shredded-archive/run=2012-12-11-01-11-33')
    >>> extract_run_id('shredded-archive/run=2012-13-11-01-11-33/')
    """
    filename = key.split('/')[-2]  # -1 element is empty string
    run_id = filename.lstrip('run=')
    try:
        datetime.strptime(run_id, '%Y-%m-%d-%H-%M-%S')
        return key
    except ValueError:
        return None


def normalize_prefix(path):
    """Add trailing slash to prefix if it is not present

    >>> normalize_prefix("somepath")
    'somepath/'
    >>> normalize_prefix("somepath/")
    'somepath/'
    """
    if path is None or path is '' or path is '/':
        return None
    elif path.endswith('/'):
        return path
    else:
        return path + '/'


def clean_dict(dict):
    """Remove all keys with Nones as values

    >>> clean_dict({'key': None})
    {}
    >>> clean_dict({'empty_s': ''})
    {'empty_s': ''}
    """
    if sys.version_info[0] < 3:
        return {k: v for k, v in dict.iteritems() if v is not None}
    else:
        return {k: v for k, v in dict.items() if v is not None}


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
