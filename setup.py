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
    Authors: Fred Blundun
    Copyright: Copyright (c) 2016-2017 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""

from distutils.core import setup

setup(
    name='snowplow_analytics_sdk',
    version='0.2.0',
    description='Snowplow Analytics Python SDK',
    author='Fred Blundun',
    url='https://github.com/snowplow/snowplow-python-analytics-sdk',
    author_email='support@snowplowanalytics.com',
    packages=['snowplow_analytics_sdk'],
    install_requires=[
        "boto3>=1.4.0",
    ]
)
