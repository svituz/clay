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

import simplejson

# Package Imports
from . import Serializer
from .. import schema_from_name


class JSONSerializer(Serializer):
    def __init__(self, message_type, schema_catalog):
        super(JSONSerializer, self).__init__(message_type, schema_catalog)
        schema_id, schema = schema_from_name(message_type, schema_catalog)
        self.schema_id = schema_id

    def serialize(self, datum):
        data = {"id": self.schema_id, "payload": datum}
        return simplejson.dumps(data)

    @staticmethod
    def deserialize(message, catalog):
        data = simplejson.loads(message)
        payload = data["payload"]
        schema_id = data["id"]
        schema = catalog[schema_id]

        return payload, schema_id, schema