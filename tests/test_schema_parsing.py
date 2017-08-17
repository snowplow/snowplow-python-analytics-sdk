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

from snowplow_analytics_sdk.json_shredder import parse_schema, fix_schema


def test_schema_parse():
    input_schema = "iglu:com.snowplowanalytics.snowplow/WebPage/jsonschema/1-0-0"
    expected_vendor = "com.snowplowanalytics.snowplow"
    expected_name = "web_page"
    expected_format = "jsonschema"
    expected_version = "1-0-0"

    vendor, name, format, version = parse_schema(input_schema, underscore_vendor=False)
    assert(vendor == expected_vendor)
    assert(name == expected_name)
    assert(format == expected_format)
    assert(version == expected_version)


def test_schema_parse_underscore_vendor():
    input_schema = "iglu:com.snowplowanalytics.snowplow/WebPage/jsonschema/1-0-0"
    expected_vendor = "com_snowplowanalytics_snowplow"
    expected_name = "web_page"
    expected_format = "jsonschema"
    expected_version = "1-0-0"

    vendor, name, format, version = parse_schema(input_schema, underscore_vendor=True)
    assert(vendor == expected_vendor)
    assert(name == expected_name)
    assert(format == expected_format)
    assert(version == expected_version)


def test_fix_schema_elasticsearch_contexts():
    input_schema = "iglu:com.snowplowanalytics.snowplow/WebPage/jsonschema/1-0-0"
    expected_string = "contexts_com_snowplowanalytics_snowplow_web_page_1"
    actual_string = fix_schema("contexts", input_schema)
    assert(actual_string == expected_string)


def test_fix_schema_elasticsearch_unstruct():
    input_schema = "iglu:com.snowplowanalytics.snowplow/WebPage/jsonschema/1-0-0"
    expected_string = "unstruct_event_com_snowplowanalytics_snowplow_web_page_1"
    actual_string = fix_schema("unstruct_event", input_schema)
    assert(actual_string == expected_string)
