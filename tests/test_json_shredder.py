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
    Authors: Devesh Shetty
    Copyright: Copyright (c) 2016 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""

from snowplow_analytics_sdk.json_shredder import parse_contexts, parse_unstruct


def test_parse_contexts():
    actual = """{
      "schema": "any",
      "data": [
        {
          "schema": "iglu:com.acme/duplicated/jsonschema/20-0-5",
          "data": {
            "value": 1
          }
        },
        {
          "schema": "iglu:com.acme/duplicated/jsonschema/20-0-5",
          "data": {
            "value": 2
          }
        },
        {
          "schema": "iglu:com.acme/unduplicated/jsonschema/1-0-0",
          "data": {
            "type": "test"
          }
        }
      ]
    }"""
    expected = [("contexts_com_acme_duplicated_20", [{"value": 1}, {"value": 2}]),
                ("contexts_com_acme_unduplicated_1", [{"type": "test"}])]

    parsed_context = parse_contexts(actual);
    assert (sorted(parsed_context) == sorted(expected))


def test_unstruct():
    actual = """{
      "schema": "any",
      "data": {
        "schema": "iglu:com.snowplowanalytics.snowplow/social_interaction/jsonschema/1-0-0",
        "data": {
          "action": "like",
          "network": "fb"
        }
      }
    }"""
    expected = [
        ("unstruct_event_com_snowplowanalytics_snowplow_social_interaction_1", {"action": "like", "network": "fb"})]
    assert (parse_unstruct(actual) == expected)
