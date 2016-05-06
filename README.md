*Work in progress*

[![License][license-image]][license]

# Snowplow Python Analytics SDK

## Overview

The **[Snowplow][snowplow]** Analytics SDK for Python lets you work with **[Snowplow enriched events] [enriched-events]** in your Python event processing and data modeling jobs.

Use this SDK with **[Apache Spark] [spark]**, **[AWS Lambda] [lambda]**, **[Databricks][databricks]** and other Python-compatible data processing frameworks.

## Functionality

The Snowplow enriched event is a relatively complex TSV string containing self-describing JSONs. Rather than work with this structure directly in Scala, use this Analytics SDK to interact with the enriched event format:

![sdk-usage-img] [sdk-usage-img]

As the Snowplow enriched event format evolves towards a cleaner **[Apache Avro] [avro]**-based structure, we will be updating this Analytics SDK to maintain compatibility across different enriched event versions.

Currently the Analytics SDK for Scala ships with a single Event Transformer:

* The JSON Event Transformer takes a Snowplow enriched event and converts it into a JSON ready for further processing

### The JSON Event Transformer

The JSON Event Transformer is adapted from the code used to load Snowplow events into Elasticsearch in the Kinesis real-time pipeline.

It converts a Snowplow enriched event into a single JSON like so:

```json
{ "app_id":"demo","platform":"web","etl_tstamp":"2015-12-01T08:32:35.048Z",
  "collector_tstamp":"2015-12-01T04:00:54.000Z","dvce_tstamp":"2015-12-01T03:57:08.986Z",
  "event":"page_view","event_id":"f4b8dd3c-85ef-4c42-9207-11ef61b2a46e",
  "name_tracker":"co","v_tracker":"js-2.5.0","v_collector":"clj-1.0.0-tom-0.2.0",...
```

The most complex piece of processing is the handling of the self-describing JSONs found in the enriched event's `unstruct_event`, `contexts` and `derived_contexts` fields. All self-describing JSONs found in the event are flattened into top-level plain (i.e. not self-describing) objects within the enriched event JSON.


For example, if an enriched event contained a `com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1`, then the final JSON would contain:

```json
{ "app_id":"demo","platform":"web","etl_tstamp":"2015-12-01T08:32:35.048Z",
  "unstruct_event_com_snowplowanalytics_snowplow_link_click_1": {
    "targetUrl":"http://www.example.com",
    "elementClasses":["foreground"],
    "elementId":"exampleLink"
  },...
```

## Usage

### Installation

Install the library like this:

```bash
pip install snowplow_analytics_sdk
```

### Using the library

```python
import snowplow_analytics_sdk.event_transformer
import snowplow_analytics_sdk.snowplow_event_transformation_exception

try:
    print(snowplow_analytics_sdk.event_transformer.transform(my_enriched_event_tsv))
except snowplow_analytics_sdk.snowplow_event_transformation_exception.SnowplowEventTransformationException as e:
    for error_message in e.error_messages:
        print(error_message)

```

### Running the tests

Run `python -m pytest` from the project root.

## 5. Copyright and license

The Snowplow Python Analytics SDK is copyright 2016 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0] [license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: http://snowplowanalytics.com
[enriched-events]: https://github.com/snowplow/snowplow/wiki/canonical-event-model
[databricks]: https://databricks.com/
[sdk-usage-img]: https://raw.githubusercontent.com/snowplow/snowplow-scala-analytics-sdk/master/sdk-usage.png
[avro]: https://avro.apache.org/
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0
