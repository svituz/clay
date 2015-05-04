#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2015, CRS4
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


from distutils.core import setup
from distutils.errors import DistutilsSetupError

import clay

desc = "CLay: Communication Layer library "

long_desc = """
CLay is a Communication Layer library that offers high level API for messaging through several messaging protocol and
serializer.
"""


def _get_version():
    try:
        with open("VERSION") as f:
            return f.read().strip()
    except IOError:
        raise DistutilsSetupError("failed to read version info")


setup(
    name="clay",
    version=_get_version(),
    description=desc,
    long_description=long_desc,
    author=clay.__author__,
    author_email=clay.__author_email__,
    url=clay.__url__,
    download_url="https://github.com/crs4/clay",
    license="MIT License",
    keywords=["Messaging", "AMQP", "MQTT", "Kafka", "Avro", "JSON"],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering"
    ],
    packages=["clay", "clay.messenger", "clay.serializer"],
    test_suite="tests",
)
