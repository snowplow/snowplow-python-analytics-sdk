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

from snowplow_analytics_sdk.event_transformer import transform
from snowplow_analytics_sdk.snowplow_event_transformation_exception import SnowplowEventTransformationException
import json

def test_domain_sessionid_exists_when_present():
    with_session_id = 'lorem.web	web	2022-08-16 11:59:51.383	2022-08-16 11:59:49.999	2022-08-16 11:59:48.380	page_view	02326d17-3a21-47d3-8e07-1630e0a680b1		plg	js-3.1.3	ssc-2.6.1-kinesis	snowplow-stream-enrich-3.2.3-common-3.2.3				9526d850-f41c-4629-8039-99ec3421d305	2	62770862-087c-4dc8-bd02-56dac553bfc9												https://www.lorem.de/ipsum/dolor/	Lorem ipsum dolor: sit amet | lorem	https://www.google.com/	https	www.lorem.de	443	/lorem/ipsum/			https	www.google.com	443	/			search	Google							{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"96ea7523-f5e9-4cd6-b0fe-c603abbe1b57"}},{"schema":"iglu:plg/user/jsonschema/2-0-5","data":{"lorem":"ipsum:dolor:sitamet"}},{"schema":"iglu:com.google.analytics/cookies/jsonschema/1-0-0","data":{"_ga":"GA1.2.1829871654.1660651188"}}]}																									Mozilla/5.0 (Linux; Android 12; SM-A515F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Mobile Safari/537.36	Chrome Mobile	Chrome	104.0.0.0	Browser (mobile)	WEBKIT	de										1	24	412	777	Android 1.x	Android	Google Inc.	Europe/Berlin	Mobile	1	412	915	UTF-8	412	6741												2022-08-16 11:59:48.396			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-3","data":{"deviceBrand":"Samsung","deviceName":"Samsung SM-A515F","operatingSystemVersionMajor":"12","layoutEngineNameVersion":"Blink 104.0","operatingSystemNameVersion":"Android 12","layoutEngineNameVersionMajor":"Blink 104","operatingSystemName":"Android","agentVersionMajor":"104","layoutEngineVersionMajor":"104","deviceClass":"Phone","agentNameVersionMajor":"Chrome 104","operatingSystemNameVersionMajor":"Android 12","operatingSystemClass":"Mobile","layoutEngineName":"Blink","agentName":"Chrome","agentVersion":"104","layoutEngineClass":"Browser","agentNameVersion":"Chrome 104","operatingSystemVersion":"12","agentClass":"Browser","layoutEngineVersion":"104.0"}},{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome Mobile","useragentMajor":"104","useragentMinor":"0","useragentPatch":"0","useragentVersion":"Chrome Mobile 104.0.0","osFamily":"Android","osMajor":"12","osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Android 12","deviceFamily":"Samsung SM-A515F"}},{"schema":"iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0","data":{"spiderOrRobot":false,"category":"BROWSER","reason":"PASSED_ALL","primaryImpact":"NONE"}}]}	cfa931dd-f465-4a78-91f3-1ea4425b038d	2022-08-16 11:59:49.983	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	81cd43dc3cad1a788221b8b58aff0615	'
    without_session_id = 'lorem.web	web	2022-08-16 11:59:51.383	2022-08-16 11:59:49.999	2022-08-16 11:59:48.380	page_view	02326d17-3a21-47d3-8e07-1630e0a680b1		plg	js-3.1.3	ssc-2.6.1-kinesis	snowplow-stream-enrich-3.2.3-common-3.2.3				9526d850-f41c-4629-8039-99ec3421d305	2	62770862-087c-4dc8-bd02-56dac553bfc9												https://www.lorem.de/ipsum/dolor/	Lorem ipsum dolor: sit amet | lorem	https://www.google.com/	https	www.lorem.de	443	/lorem/ipsum/			https	www.google.com	443	/			search	Google							{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"96ea7523-f5e9-4cd6-b0fe-c603abbe1b57"}},{"schema":"iglu:plg/user/jsonschema/2-0-5","data":{"lorem":"ipsum:dolor:sitamet"}},{"schema":"iglu:com.google.analytics/cookies/jsonschema/1-0-0","data":{"_ga":"GA1.2.1829871654.1660651188"}}]}																									Mozilla/5.0 (Linux; Android 12; SM-A515F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Mobile Safari/537.36	Chrome Mobile	Chrome	104.0.0.0	Browser (mobile)	WEBKIT	de										1	24	412	777	Android 1.x	Android	Google Inc.	Europe/Berlin	Mobile	1	412	915	UTF-8	412	6741												2022-08-16 11:59:48.396			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-3","data":{"deviceBrand":"Samsung","deviceName":"Samsung SM-A515F","operatingSystemVersionMajor":"12","layoutEngineNameVersion":"Blink 104.0","operatingSystemNameVersion":"Android 12","layoutEngineNameVersionMajor":"Blink 104","operatingSystemName":"Android","agentVersionMajor":"104","layoutEngineVersionMajor":"104","deviceClass":"Phone","agentNameVersionMajor":"Chrome 104","operatingSystemNameVersionMajor":"Android 12","operatingSystemClass":"Mobile","layoutEngineName":"Blink","agentName":"Chrome","agentVersion":"104","layoutEngineClass":"Browser","agentNameVersion":"Chrome 104","operatingSystemVersion":"12","agentClass":"Browser","layoutEngineVersion":"104.0"}},{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome Mobile","useragentMajor":"104","useragentMinor":"0","useragentPatch":"0","useragentVersion":"Chrome Mobile 104.0.0","osFamily":"Android","osMajor":"12","osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Android 12","deviceFamily":"Samsung SM-A515F"}},{"schema":"iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0","data":{"spiderOrRobot":false,"category":"BROWSER","reason":"PASSED_ALL","primaryImpact":"NONE"}}]}		2022-08-16 11:59:49.983	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	81cd43dc3cad1a788221b8b58aff0615	'

    transformed_with_session_id = transform(with_session_id)
    transformed_without_session_id = transform(without_session_id)

    assert(transformed_with_session_id.get('domain_sessionid') == 'cfa931dd-f465-4a78-91f3-1ea4425b038d')
    assert(transformed_without_session_id.get('domain_sessionid') == None)
    assert(transformed_with_session_id != transformed_without_session_id)
