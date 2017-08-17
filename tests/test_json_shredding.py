"""
    Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
    This program is licensed to you under the Apache License Version 2.0,
    and you may not use this file except in compliance with the Apache License
    Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
    http://www.apache.org/licenses/LICENSE-2.0.
    Unless required by applicable law or agreed to in writing,
    software distributed under the Apache License Version 2.0 is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
    express or implied. See the Apache License Version 2.0 for the specific
    language governing permissions and limitations there under.
    Authors: Mike Robins
    Copyright: Copyright (c) 2017 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""

from snowplow_analytics_sdk.json_shredder import parse_contexts, parse_unstruct

def test_parse_contexts_redshift():
    json_input = """{
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
    }"""

    expected = [
            ('com_acme_unduplicated_1', [{
                'schema':
                    {
                        'version': '1-0-0',
                        'vendor': 'com.acme',
                        'name': 'unduplicated',
                        'format': 'jsonschema'
                    },
                'data': {
                    'unique': True
                },
                        }]),
        ('com_acme_duplicated_1', [
            {
                'schema':
                    {
                        'version': '1-0-0',
                        'vendor': 'com.acme',
                        'name': 'duplicated',
                        'format': 'jsonschema'
                    },
                'data': {
                    'value': 1
                }
            },
            {
                'schema':
                    {
                        'version': '1-0-0',
                        'vendor': 'com.acme',
                        'name': 'duplicated',
                        'format': 'jsonschema'
                    },
                'data': {
                    'value': 2
                },
            }
        ])
    ]
    result = parse_contexts(json_input, shred_format='redshift')
    assert(sorted(result) == sorted(expected))

def test_parse_contexts_elasticsearch():
    json_input = """{
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
    }"""

    expected = [
        ('contexts_com_acme_unduplicated_1', [{'unique': True}]),
        ('contexts_com_acme_duplicated_1', [{'value': 1}, {'value': 2}])
    ]
    result = parse_contexts(json_input, shred_format='elasticsearch')
    assert(sorted(result) == sorted(expected))

def test_parse_unstruct_redshift():
    json_input = """{
      "data": {
        "data": {
          "key": "value"
        },
        "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
      },
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"
    }"""
    expected = [
      (
        "com_snowplowanalytics_snowplow_link_click_1", {
          "schema": {
            "vendor": "com.snowplowanalytics.snowplow",
            "name": "link_click",
            "format": "jsonschema",
            "version": "1-0-1"
          },
          "data": {
            "key": "value"
          }
        }
      )
    ]

    result = parse_unstruct(json_input, shred_format='redshift')
    assert(result == expected)

def test_parse_unstruct_elasticsearch():
    json_input = """{
      "data": {
        "data": {
          "key": "value"
        },
        "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
      },
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"
    }"""
    expected = [
      (
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1", {
          "key": "value"
        }
      )
    ]

    result = parse_unstruct(json_input, shred_format='elasticsearch')
    assert(result == expected)