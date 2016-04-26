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

from snowplow_analytics_sdk import json_shredder
from snowplow_analytics_sdk.snowplow_event_transformation_exception import SnowplowEventTransformationException

LATITUDE_INDEX = 22
LONGITUDE_INDEX = 23


def convert_string(key, value):
    return [(key, value)]


def convert_int(key, value):
    return [(key, int(value))]


def convert_bool(key, value):
    if value == '1':
        return [(key, True)]
    elif value == '0':
        return [(key, False)]
    raise SnowplowEventTransformationException(["Invalid value {} for field {}".format(value, key)])


def convert_double(key, value):
    return [(key, float(value))]


def convert_tstamp(key, value):
    return [(key, value.replace(' ', 'T') + 'Z')]


def convert_contexts(key, value):
    return json_shredder.parse_contexts(value)


def convert_unstruct(key, value):
    return json_shredder.parse_unstruct(value)

# Ordered list of names of enriched event fields together with the function required to convert them to JSON
ENRICHED_EVENT_FIELD_TYPES = (
    ("app_id", convert_string),
    ("platform", convert_string),
    ("etl_tstamp", convert_tstamp),
    ("collector_tstamp", convert_tstamp),
    ("dvce_created_tstamp", convert_tstamp),
    ("event", convert_string),
    ("event_id", convert_string),
    ("txn_id", convert_int),
    ("name_tracker", convert_string),
    ("v_tracker", convert_string),
    ("v_collector", convert_string),
    ("v_etl", convert_string),
    ("user_id", convert_string),
    ("user_ipaddress", convert_string),
    ("user_fingerprint", convert_string),
    ("domain_userid", convert_string),
    ("domain_sessionidx", convert_int),
    ("network_userid", convert_string),
    ("geo_country", convert_string),
    ("geo_region", convert_string),
    ("geo_city", convert_string),
    ("geo_zipcode", convert_string),
    ("geo_latitude", convert_double),
    ("geo_longitude", convert_double),
    ("geo_region_name", convert_string),
    ("ip_isp", convert_string),
    ("ip_organization", convert_string),
    ("ip_domain", convert_string),
    ("ip_netspeed", convert_string),
    ("page_url", convert_string),
    ("page_title", convert_string),
    ("page_referrer", convert_string),
    ("page_urlscheme", convert_string),
    ("page_urlhost", convert_string),
    ("page_urlport", convert_int),
    ("page_urlpath", convert_string),
    ("page_urlquery", convert_string),
    ("page_urlfragment", convert_string),
    ("refr_urlscheme", convert_string),
    ("refr_urlhost", convert_string),
    ("refr_urlport", convert_int),
    ("refr_urlpath", convert_string),
    ("refr_urlquery", convert_string),
    ("refr_urlfragment", convert_string),
    ("refr_medium", convert_string),
    ("refr_source", convert_string),
    ("refr_term", convert_string),
    ("mkt_medium", convert_string),
    ("mkt_source", convert_string),
    ("mkt_term", convert_string),
    ("mkt_content", convert_string),
    ("mkt_campaign", convert_string),
    ("contexts", convert_contexts),
    ("se_category", convert_string),
    ("se_action", convert_string),
    ("se_label", convert_string),
    ("se_property", convert_string),
    ("se_value", convert_string),
    ("unstruct_event", convert_unstruct),
    ("tr_orderid", convert_string),
    ("tr_affiliation", convert_string),
    ("tr_total", convert_double),
    ("tr_tax", convert_double),
    ("tr_shipping", convert_double),
    ("tr_city", convert_string),
    ("tr_state", convert_string),
    ("tr_country", convert_string),
    ("ti_orderid", convert_string),
    ("ti_sku", convert_string),
    ("ti_name", convert_string),
    ("ti_category", convert_string),
    ("ti_price", convert_double),
    ("ti_quantity", convert_int),
    ("pp_xoffset_min", convert_int),
    ("pp_xoffset_max", convert_int),
    ("pp_yoffset_min", convert_int),
    ("pp_yoffset_max", convert_int),
    ("useragent", convert_string),
    ("br_name", convert_string),
    ("br_family", convert_string),
    ("br_version", convert_string),
    ("br_type", convert_string),
    ("br_renderengine", convert_string),
    ("br_lang", convert_string),
    ("br_features_pdf", convert_bool),
    ("br_features_flash", convert_bool),
    ("br_features_java", convert_bool),
    ("br_features_director", convert_bool),
    ("br_features_quicktime", convert_bool),
    ("br_features_realplayer", convert_bool),
    ("br_features_windowsmedia", convert_bool),
    ("br_features_gears", convert_bool),
    ("br_features_silverlight", convert_bool),
    ("br_cookies", convert_bool),
    ("br_colordepth", convert_string),
    ("br_viewwidth", convert_int),
    ("br_viewheight", convert_int),
    ("os_name", convert_string),
    ("os_family", convert_string),
    ("os_manufacturer", convert_string),
    ("os_timezone", convert_string),
    ("dvce_type", convert_string),
    ("dvce_ismobile", convert_bool),
    ("dvce_screenwidth", convert_int),
    ("dvce_screenheight", convert_int),
    ("doc_charset", convert_string),
    ("doc_width", convert_int),
    ("doc_height", convert_int),
    ("tr_currency", convert_string),
    ("tr_total_base", convert_double),
    ("tr_tax_base", convert_double),
    ("tr_shipping_base", convert_double),
    ("ti_currency", convert_string),
    ("ti_price_base", convert_double),
    ("base_currency", convert_string),
    ("geo_timezone", convert_string),
    ("mkt_clickid", convert_string),
    ("mkt_network", convert_string),
    ("etl_tags", convert_string),
    ("dvce_sent_tstamp", convert_tstamp),
    ("refr_domain_userid", convert_string),
    ("refr_device_tstamp", convert_tstamp),
    ("derived_contexts", convert_contexts),
    ("domain_sessionid", convert_string),
    ("derived_tstamp", convert_tstamp),
    ("event_vendor", convert_string),
    ("event_name", convert_string),
    ("event_format", convert_string),
    ("event_version", convert_string),
    ("event_fingerprint", convert_string),
    ("true_tstamp", convert_tstamp)
)


def transform(line, known_fields=ENRICHED_EVENT_FIELD_TYPES, add_geolocation_data=True):
    """
    Convert a Snowplow enriched event TSV into a JSON
    """
    return jsonify_good_event(line.split('\t'), known_fields, add_geolocation_data)


def jsonify_good_event(event, known_fields=ENRICHED_EVENT_FIELD_TYPES, add_geolocation_data=True):
    """
    Convert a Snowplow enriched event in the form of an array of fields into a JSON
    """
    if len(event) != len(known_fields):
        raise SnowplowEventTransformationException(
            ["Expected {} fields, received {} fields.".format(len(known_fields), len(event))]
        )
    else:
        output = {}
        errors = []
        if add_geolocation_data and event[LATITUDE_INDEX] != '' and event[LONGITUDE_INDEX] != '':
            output['geo_location'] = event[LATITUDE_INDEX] + ',' + event[LONGITUDE_INDEX]
        for i in range(len(event)):
            key = known_fields[i][0]
            if event[i] != '':
                try:
                    kvpairs = known_fields[i][1](key, event[i])
                    for kvpair in kvpairs:
                        output[kvpair[0]] = kvpair[1]
                except SnowplowEventTransformationException as sete:
                    errors += sete.error_messages
                except Exception as e:
                    errors += ["Unexpected exception parsing field with key {} and value {}: {}".format(
                        known_fields[i][0],
                        event[i],
                        repr(e)
                    )]
        if errors:
            raise SnowplowEventTransformationException(errors)
        else:
            return output
