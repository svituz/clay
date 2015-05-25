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

from unittest import TestCase

from clay.factory import MessageFactory
from clay.serializer import JSONSerializer

from tests import TEST_CATALOG


class TestJSON(TestCase):
    def setUp(self):
        self.factory = MessageFactory(JSONSerializer, TEST_CATALOG)

        self.simple_message = self.factory.create('TEST')
        self.simple_message.id = 1111111
        self.simple_message.name = "aaa"
        self.simple_encoded = '{"id": 0, "payload": {"id": 1111111, "name": "aaa"}}'

        self.complex_message = self.factory.create('TEST_COMPLEX')
        self.complex_message.id = 1111111
        self.complex_message.name = "aaa"
        self.complex_message.array_complex_field.add()
        self.complex_message.array_complex_field[0].field_1 = "bbb"
        self.complex_message.array_simple_field.add()
        self.complex_message.array_simple_field[0] = "ccc"
        self.complex_message.record_field.field_1 = "ddd"
        self.complex_message.record_field.field_2 = "eee"

        self.complex_encoded = '{"id": 1, "payload": {"record_field": {"field_2": "eee", "field_1": "ddd"}, ' \
                               '"array_simple_field": ["ccc"], "array_complex_field": [{"field_1": "bbb"}], ' \
                               '"id": 1111111, "name": "aaa"}}'

    def test_retrieve(self):
        m = self.factory.retrieve(self.complex_encoded)
        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], "ccc")
        self.assertEqual(m.record_field.field_1, "ddd")
        self.assertEqual(m.record_field.field_2, "eee")

    def test_serializer(self):
        value = self.simple_message.serialize()
        self.assertEqual(value, self.simple_encoded)

        value = self.complex_message.serialize()
        self.assertEqual(value, self.complex_encoded)
