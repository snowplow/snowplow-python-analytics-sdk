"""
    Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
    This program is licensed to you under the Apache License Version 2.0,
    and you may not use this file except in compliance with the Apache License
    Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
    http://www.apache.org/licenses/LICENSE-2.0.
    Unless required by applicable law or agreed to in writing,
    software distributed under the Apache License Version 2.0 is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
    express or implied. See the Apache License Version 2.0 for the specific
    language governing permissions and limitations there under.
    Authors: Fred Blundun
    Copyright: Copyright (c) 2016 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""

import re
import json
from snowplow_analytics_sdk.snowplow_event_transformation_exception import SnowplowEventTransformationException


# TODO: remove in 0.3.0
SCHEMA_PATTERN = re.compile(""".+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""")

SCHEMA_URI = ("^iglu:"                        # Protocol
              "([a-zA-Z0-9-_.]+)/"            # Vendor
              "([a-zA-Z0-9-_]+)/"             # Name
              "([a-zA-Z0-9-_]+)/"             # Format
              "([1-9][0-9]*"                  # MODEL (cannot start with 0)
              "(?:-(?:0|[1-9][0-9]*)){2})$")  # REVISION and ADDITION

SCHEMA_URI_REGEX = re.compile(SCHEMA_URI)


def extract_schema(uri):
    """
    Extracts Schema information from Iglu URI

    >>> extract_schema("iglu:com.acme-corporation_under/event_name-dash/jsonschema/1-10-1")['vendor']
    'com.acme-corporation_under'
    """
    match = re.match(SCHEMA_URI_REGEX, uri)
    if match:
        return {
            'vendor': match.group(1),
            'name': match.group(2),
            'format': match.group(3),
            'version': match.group(4)

        }
    else:
        raise SnowplowEventTransformationException([
            "Schema {} does not conform to regular expression {}".format(uri, SCHEMA_URI)
        ])


def fix_schema(prefix, schema):
    """
    Create an Elasticsearch field name from a schema string
    """
    schema_dict = extract_schema(schema)
    snake_case_organization = schema_dict['vendor'].replace('.', '_').lower()
    snake_case_name = re.sub('([^A-Z_])([A-Z])', '\g<1>_\g<2>', schema_dict['name']).lower()
    model = schema_dict['version'].split('-')[0]
    return "{}_{}_{}_{}".format(prefix, snake_case_organization, snake_case_name, model)


def parse_contexts(contexts):
    """
    Convert a contexts JSON to an Elasticsearch-compatible list of key-value pairs
    For example, the JSON

    {
      "data": [
        {
          "data": {
            "unique": true
          },
          "schema": "iglu:com.acme/unduplicated/jsonschema/1-0-0"
        },
        {
          "data": {
            "value": 1
          },
          "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0"
        },
        {
          "data": {
            "value": 2
          },
          "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0"
        }
      ],
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"
    }

    would become

    [
      ("context_com_acme_duplicated_1", [{"value": 1}, {"value": 2}]),
      ("context_com_acme_unduplicated_1", [{"unique": true}])
    ]
    """
    my_json = json.loads(contexts)
    data = my_json['data']
    distinct_contexts = {}
    for context in data:
        schema = fix_schema("contexts", context['schema'])
        inner_data = context['data']
        if schema not in distinct_contexts:
            distinct_contexts[schema] = [inner_data]
        else:
            distinct_contexts[schema].append(inner_data)
    output = []
    for key in distinct_contexts:
        output.append((key, distinct_contexts[key]))
    return output


def parse_unstruct(unstruct):
    """
    Convert an unstructured event JSON to a list containing one Elasticsearch-compatible key-value pair
    For example, the JSON

    {
      "data": {
        "data": {
          "key": "value"
        },
        "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
      },
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"
    }

    would become

    [
      (
        "unstruct_com_snowplowanalytics_snowplow_link_click_1", {
          "key": "value"
        }
      )
    ]
    """
    my_json = json.loads(unstruct)
    data = my_json['data']
    schema = data['schema']
    if 'data' in data:
        inner_data = data['data']
    else:
        raise SnowplowEventTransformationException(["Could not extract inner data field from unstructured event"])
    fixed_schema = fix_schema("unstruct_event", schema)
    return [(fixed_schema, inner_data)]
