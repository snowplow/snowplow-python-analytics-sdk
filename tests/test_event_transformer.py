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

unstruct_json = """{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": {
      "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1",
      "data": {
        "targetUrl": "http://www.example.com",
        "elementClasses": ["foreground"],
        "elementId": "exampleLink"
      }
    }
  }"""

contexts_json = """{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": [
      {
        "schema": "iglu:org.schema/WebPage/jsonschema/1-0-0",
        "data": {
          "genre": "blog",
          "inLanguage": "en-US",
          "datePublished": "2014-11-06T00:00:00Z",
          "author": "Fred Blundun",
          "breadcrumb": [
            "blog",
            "releases"
          ],
          "keywords": [
            "snowplow",
            "javascript",
            "tracker",
            "event"
          ]
        }
      },
      {
        "schema": "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0",
        "data": {
          "navigationStart": 1415358089861,
          "unloadEventStart": 1415358090270,
          "unloadEventEnd": 1415358090287,
          "redirectStart": 0,
          "redirectEnd": 0,
          "fetchStart": 1415358089870,
          "domainLookupStart": 1415358090102,
          "domainLookupEnd": 1415358090102,
          "connectStart": 1415358090103,
          "connectEnd": 1415358090183,
          "requestStart": 1415358090183,
          "responseStart": 1415358090265,
          "responseEnd": 1415358090265,
          "domLoading": 1415358090270,
          "domInteractive": 1415358090886,
          "domContentLoadedEventStart": 1415358090968,
          "domContentLoadedEventEnd": 1415358091309,
          "domComplete": 0,
          "loadEventStart": 0,
          "loadEventEnd": 0
        }
      }
    ]
  }"""

derived_contexts_json = """{
    "schema": "iglu:com.snowplowanalytics.snowplow\/contexts\/jsonschema\/1-0-1",
    "data": [
      {
        "schema": "iglu:com.snowplowanalytics.snowplow\/ua_parser_context\/jsonschema\/1-0-0",
        "data": {
          "useragentFamily": "IE",
          "useragentMajor": "7",
          "useragentMinor": "0",
          "useragentPatch": null,
          "useragentVersion": "IE 7.0",
          "osFamily": "Windows XP",
          "osMajor": null,
          "osMinor": null,
          "osPatch": null,
          "osPatchMinor": null,
          "osVersion": "Windows XP",
          "deviceFamily": "Other"
        }
      }
    ]
  }"""

full_event = (
        ("app_id", "angry-birds"),
        ("platform", "web"),
        ("etl_tstamp", "2017-01-26 00:01:25.292"),
        ("collector_tstamp", "2013-11-26 00:02:05"),
        ("dvce_created_tstamp", "2013-11-26 00:03:57.885"),
        ("event", "page_view"),
        ("event_id", "c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        ("txn_id", "41828"),
        ("name_tracker", "cloudfront-1"),
        ("v_tracker", "js-2.1.0"),
        ("v_collector", "clj-tomcat-0.1.0"),
        ("v_etl", "serde-0.5.2"),
        ("user_id", "jon.doe@email.com"),
        ("user_ipaddress", "92.231.54.234"),
        ("user_fingerprint", "2161814971"),
        ("domain_userid", "bc2e92ec6c204a14"),
        ("domain_sessionidx", "3"),
        ("network_userid", "ecdff4d0-9175-40ac-a8bb-325c49733607"),
        ("geo_country", "US"),
        ("geo_region", "TX"),
        ("geo_city", "New York"),
        ("geo_zipcode", "94109"),
        ("geo_latitude", "37.443604"),
        ("geo_longitude", "-122.4124"),
        ("geo_region_name", "Florida"),
        ("ip_isp", "FDN Communications"),
        ("ip_organization", "Bouygues Telecom"),
        ("ip_domain", "nuvox.net"),
        ("ip_netspeed", "Cable/DSL"),
        ("page_url", "http://www.snowplowanalytics.com"),
        ("page_title", "On Analytics"),
        ("page_referrer", ""),
        ("page_urlscheme", "http"),
        ("page_urlhost", "www.snowplowanalytics.com"),
        ("page_urlport", "80"),
        ("page_urlpath", "/product/index.html"),
        ("page_urlquery", "id=GTM-DLRG"),
        ("page_urlfragment", "4-conclusion"),
        ("refr_urlscheme", ""),
        ("refr_urlhost", ""),
        ("refr_urlport", ""),
        ("refr_urlpath", ""),
        ("refr_urlquery", ""),
        ("refr_urlfragment", ""),
        ("refr_medium", ""),
        ("refr_source", ""),
        ("refr_term", ""),
        ("mkt_medium", ""),
        ("mkt_source", ""),
        ("mkt_term", ""),
        ("mkt_content", ""),
        ("mkt_campaign", ""),
        ("contexts", contexts_json),
        ("se_category", ""),
        ("se_action", ""),
        ("se_label", ""),
        ("se_property", ""),
        ("se_value", ""),
        ("unstruct_event", unstruct_json),
        ("tr_orderid", ""),
        ("tr_affiliation", ""),
        ("tr_total", ""),
        ("tr_tax", ""),
        ("tr_shipping", ""),
        ("tr_city", ""),
        ("tr_state", ""),
        ("tr_country", ""),
        ("ti_orderid", ""),
        ("ti_sku", ""),
        ("ti_name", ""),
        ("ti_category", ""),
        ("ti_price", ""),
        ("ti_quantity", ""),
        ("pp_xoffset_min", ""),
        ("pp_xoffset_max", ""),
        ("pp_yoffset_min", ""),
        ("pp_yoffset_max", ""),
        ("useragent", ""),
        ("br_name", ""),
        ("br_family", ""),
        ("br_version", ""),
        ("br_type", ""),
        ("br_renderengine", ""),
        ("br_lang", ""),
        ("br_features_pdf", "1"),
        ("br_features_flash", "0"),
        ("br_features_java", ""),
        ("br_features_director", ""),
        ("br_features_quicktime", ""),
        ("br_features_realplayer", ""),
        ("br_features_windowsmedia", ""),
        ("br_features_gears", ""),
        ("br_features_silverlight", ""),
        ("br_cookies", ""),
        ("br_colordepth", ""),
        ("br_viewwidth", ""),
        ("br_viewheight", ""),
        ("os_name", ""),
        ("os_family", ""),
        ("os_manufacturer", ""),
        ("os_timezone", ""),
        ("dvce_type", ""),
        ("dvce_ismobile", ""),
        ("dvce_screenwidth", ""),
        ("dvce_screenheight", ""),
        ("doc_charset", ""),
        ("doc_width", ""),
        ("doc_height", ""),
        ("tr_currency", ""),
        ("tr_total_base", ""),
        ("tr_tax_base", ""),
        ("tr_shipping_base", ""),
        ("ti_currency", ""),
        ("ti_price_base", ""),
        ("base_currency", ""),
        ("geo_timezone", ""),
        ("mkt_clickid", ""),
        ("mkt_network", ""),
        ("etl_tags", ""),
        ("dvce_sent_tstamp", ""),
        ("refr_domain_userid", ""),
        ("refr_device_tstamp", ""),
        ("derived_contexts", derived_contexts_json),
        ("domain_sessionid", "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"),
        ("derived_tstamp", "2013-11-26 00:03:57.886"),
        ("event_vendor", "com.snowplowanalytics.snowplow"),
        ("event_name", "link_click"),
        ("event_format", "jsonschema"),
        ("event_version", "1-0-0"),
        ("event_fingerprint", "e3dbfa9cca0412c3d4052863cefb547f"),
        ("true_tstamp", "2013-11-26 00:03:57.886")
    )

tsv = '\t'.join(map(lambda x: x[1].replace('\n', ''), full_event))

expected = json.loads("""{
        "geo_location" : "37.443604,-122.4124",
        "app_id" : "angry-birds",
        "platform" : "web",
        "etl_tstamp" : "2017-01-26T00:01:25.292Z",
        "collector_tstamp" : "2013-11-26T00:02:05Z",
        "dvce_created_tstamp" : "2013-11-26T00:03:57.885Z",
        "event" : "page_view",
        "event_id" : "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" : 41828,
        "name_tracker" : "cloudfront-1",
        "v_tracker" : "js-2.1.0",
        "v_collector" : "clj-tomcat-0.1.0",
        "v_etl" : "serde-0.5.2",
        "user_id" : "jon.doe@email.com",
        "user_ipaddress" : "92.231.54.234",
        "user_fingerprint" : "2161814971",
        "domain_userid" : "bc2e92ec6c204a14",
        "domain_sessionidx" : 3,
        "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" : "US",
        "geo_region" : "TX",
        "geo_city" : "New York",
        "geo_zipcode" : "94109",
        "geo_latitude" : 37.443604,
        "geo_longitude" : -122.4124,
        "geo_region_name" : "Florida",
        "ip_isp" : "FDN Communications",
        "ip_organization" : "Bouygues Telecom",
        "ip_domain" : "nuvox.net",
        "ip_netspeed" : "Cable/DSL",
        "page_url" : "http://www.snowplowanalytics.com",
        "page_title" : "On Analytics",
        "page_urlscheme" : "http",
        "page_urlhost" : "www.snowplowanalytics.com",
        "page_urlport" : 80,
        "page_urlpath" : "/product/index.html",
        "page_urlquery" : "id=GTM-DLRG",
        "page_urlfragment" : "4-conclusion",
        "contexts_org_schema_web_page_1" : [ {
          "genre" : "blog",
          "inLanguage" : "en-US",
          "datePublished" : "2014-11-06T00:00:00Z",
          "author" : "Fred Blundun",
          "breadcrumb" : [ "blog", "releases" ],
          "keywords" : [ "snowplow", "javascript", "tracker", "event" ]
        } ],
        "contexts_org_w3_performance_timing_1" : [ {
          "navigationStart" : 1415358089861,
          "unloadEventStart" : 1415358090270,
          "unloadEventEnd" : 1415358090287,
          "redirectStart" : 0,
          "redirectEnd" : 0,
          "fetchStart" : 1415358089870,
          "domainLookupStart" : 1415358090102,
          "domainLookupEnd" : 1415358090102,
          "connectStart" : 1415358090103,
          "connectEnd" : 1415358090183,
          "requestStart" : 1415358090183,
          "responseStart" : 1415358090265,
          "responseEnd" : 1415358090265,
          "domLoading" : 1415358090270,
          "domInteractive" : 1415358090886,
          "domContentLoadedEventStart" : 1415358090968,
          "domContentLoadedEventEnd" : 1415358091309,
          "domComplete" : 0,
          "loadEventStart" : 0,
          "loadEventEnd" : 0
        } ],
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" : {
          "targetUrl" : "http://www.example.com",
          "elementClasses" : [ "foreground" ],
          "elementId" : "exampleLink"
        },
        "br_features_pdf" : true,
        "br_features_flash" : false,
        "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{
          "useragentFamily": "IE",
          "useragentMajor": "7",
          "useragentMinor": "0",
          "useragentPatch": null,
          "useragentVersion": "IE 7.0",
          "osFamily": "Windows XP",
          "osMajor": null,
          "osMinor": null,
          "osPatch": null,
          "osPatchMinor": null,
          "osVersion": "Windows XP",
          "deviceFamily": "Other"
        }],
        "domain_sessionid": "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp": "2013-11-26T00:03:57.886Z",
        "event_vendor": "com.snowplowanalytics.snowplow",
        "event_name": "link_click",
        "event_format": "jsonschema",
        "event_version": "1-0-0",
        "event_fingerprint": "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp": "2013-11-26T00:03:57.886Z"
      }""")


def _get_expected_with_empty_fields():
    result = expected.copy()
    exclude = ('derived_contexts', 'contexts', 'unstruct_event')
    for key, value in full_event:
        if key not in exclude and key not in result:
            result[key] = None
    return result


def test_transform():
    actual = transform(tsv)
    assert(actual == expected)


def test_transform_keep_empty_fields():
    actual = transform(tsv, drop_empty_fields=False)
    assert(actual == _get_expected_with_empty_fields())


def test_wrong_tsv_length():
    exception = None
    try:
        transform("two\tfields")
    except SnowplowEventTransformationException as sete:
        exception = sete
    assert(exception.message == "Expected 131 fields, received 2 fields.")


def test_malformed_field():
    exception = None
    malformed_fields_tsv = '\t' * 110 + 'bad_tax_base' + '\t' * 20

    try:
        transform(malformed_fields_tsv)
    except SnowplowEventTransformationException as sete:
        exception = sete
    assert(exception.message.startswith(
      "Unexpected exception parsing field with key tr_tax_base and value bad_tax_base"))


def test_multiple_malformed_fields():
    exception = None
    malformed_fields_tsv = '\t' * 52 + 'bad_contexts' + '\t' * 50 + 'bad_dvce_ismobile' + '\t' * 28

    try:
        transform(malformed_fields_tsv)
    except SnowplowEventTransformationException as sete:
        exception = sete
    assert(len(exception.error_messages) == 2)