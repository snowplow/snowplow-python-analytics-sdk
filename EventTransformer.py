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

import json

def StringField(key, value):
    return (key, value)
def IntField(key, value):
    return (key, int(value))
def BoolField(key, value):
    if value == '1':
        return (key, True)
    elif value == '0':
        return (key, False)
    raise Exception("Invalid value {} for field {}".format(value, key))

def DoubleField(key, value):
    return (key, float(value))
def TstampField(key, value):
    return (key, value.replace(' ', 'T') + 'Z')
def ContextsField(key, value):
    return (key, "TODO")
def UnstructField(key, value):
    return (key, "TODO")

ACTUAL_FIELDS = (
    ("app_id", StringField),
    ("platform", StringField),
    ("etl_tstamp", TstampField),
    ("collector_tstamp", TstampField),
    ("dvce_created_tstamp", TstampField),
    ("event", StringField),
    ("event_id", StringField),
    ("txn_id", IntField),
    ("name_tracker", StringField),
    ("v_tracker", StringField),
    ("v_collector", StringField),
    ("v_etl", StringField),
    ("user_id", StringField),
    ("user_ipaddress", StringField),
    ("user_fingerprint", StringField),
    ("domain_userid", StringField),
    ("domain_sessionidx", IntField),
    ("network_userid", StringField),
    ("geo_country", StringField),
    ("geo_region", StringField),
    ("geo_city", StringField),
    ("geo_zipcode", StringField),
    ("geo_latitude", DoubleField),
    ("geo_longitude", DoubleField),
    ("geo_region_name", StringField),
    ("ip_isp", StringField),
    ("ip_organization", StringField),
    ("ip_domain", StringField),
    ("ip_netspeed", StringField),
    ("page_url", StringField),
    ("page_title", StringField),
    ("page_referrer", StringField),
    ("page_urlscheme", StringField),
    ("page_urlhost", StringField),
    ("page_urlport", IntField),
    ("page_urlpath", StringField),
    ("page_urlquery", StringField),
    ("page_urlfragment", StringField),
    ("refr_urlscheme", StringField),
    ("refr_urlhost", StringField),
    ("refr_urlport", IntField),
    ("refr_urlpath", StringField),
    ("refr_urlquery", StringField),
    ("refr_urlfragment", StringField),
    ("refr_medium", StringField),
    ("refr_source", StringField),
    ("refr_term", StringField),
    ("mkt_medium", StringField),
    ("mkt_source", StringField),
    ("mkt_term", StringField),
    ("mkt_content", StringField),
    ("mkt_campaign", StringField),
    ("contexts", ContextsField),
    ("se_category", StringField),
    ("se_action", StringField),
    ("se_label", StringField),
    ("se_property", StringField),
    ("se_value", StringField),
    ("unstruct_event", UnstructField),
    ("tr_orderid", StringField),
    ("tr_affiliation", StringField),
    ("tr_total", DoubleField),
    ("tr_tax", DoubleField),
    ("tr_shipping", DoubleField),
    ("tr_city", StringField),
    ("tr_state", StringField),
    ("tr_country", StringField),
    ("ti_orderid", StringField),
    ("ti_sku", StringField),
    ("ti_name", StringField),
    ("ti_category", StringField),
    ("ti_price", DoubleField),
    ("ti_quantity", IntField),
    ("pp_xoffset_min", IntField),
    ("pp_xoffset_max", IntField),
    ("pp_yoffset_min", IntField),
    ("pp_yoffset_max", IntField),
    ("useragent", StringField),
    ("br_name", StringField),
    ("br_family", StringField),
    ("br_version", StringField),
    ("br_type", StringField),
    ("br_renderengine", StringField),
    ("br_lang", StringField),
    ("br_features_pdf", BoolField),
    ("br_features_flash", BoolField),
    ("br_features_java", BoolField),
    ("br_features_director", BoolField),
    ("br_features_quicktime", BoolField),
    ("br_features_realplayer", BoolField),
    ("br_features_windowsmedia", BoolField),
    ("br_features_gears", BoolField),
    ("br_features_silverlight", BoolField),
    ("br_cookies", BoolField),
    ("br_colordepth", StringField),
    ("br_viewwidth", IntField),
    ("br_viewheight", IntField),
    ("os_name", StringField),
    ("os_family", StringField),
    ("os_manufacturer", StringField),
    ("os_timezone", StringField),
    ("dvce_type", StringField),
    ("dvce_ismobile", BoolField),
    ("dvce_screenwidth", IntField),
    ("dvce_screenheight", IntField),
    ("doc_charset", StringField),
    ("doc_width", IntField),
    ("doc_height", IntField),
    ("tr_currency", StringField),
    ("tr_total_base", DoubleField),
    ("tr_tax_base", DoubleField),
    ("tr_shipping_base", DoubleField),
    ("ti_currency", StringField),
    ("ti_price_base", DoubleField),
    ("base_currency", StringField),
    ("geo_timezone", StringField),
    ("mkt_clickid", StringField),
    ("mkt_network", StringField),
    ("etl_tags", StringField),
    ("dvce_sent_tstamp", TstampField),
    ("refr_domain_userid", StringField),
    ("refr_device_tstamp", TstampField),
    ("derived_contexts", ContextsField),
    ("domain_sessionid", StringField),
    ("derived_tstamp", TstampField),
    ("event_vendor", StringField),
    ("event_name", StringField),
    ("event_format", StringField),
    ("event_version", StringField),
    ("event_fingerprint", StringField),
    ("true_tstamp", TstampField)
)

def transform(line, known_fields=ACTUAL_FIELDS):
    return jsonify_good_event(line.split('\t'), known_fields)

def jsonify_good_event(event, known_fields=ACTUAL_FIELDS): # array of strings
    if len(event) != len(known_fields):
        raise Exception("Expected {} fields, received {} fields.".format(len(known_fields), len(event)))
    else:
        output = {}
        for i in range(len(event)):
            key = known_fields[i][0]
            if event[i] == '':
                output[key] = None
            else:
                json_key, json_value = known_fields[i][1](key, event[i])
                output[json_key] = json_value
        return output
