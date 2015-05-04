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

import unittest
from unittest import TestCase

import time
from multiprocessing import Process

import pika

from clay.exceptions import InvalidMessage, SchemaException, InvalidContent
from clay.factory import MessageFactory
from clay.serializer import AvroSerializer, AbstractHL7Serializer
from clay.message import _Record

from tests import TEST_CATALOG, TEST_SCHEMA


class TestMessage(TestCase):
    def setUp(self):
        self.avro_factory = MessageFactory(AvroSerializer, TEST_CATALOG)
        self.hl7_factory = MessageFactory(AbstractHL7Serializer, TEST_CATALOG)

        self.avro_message = self.avro_factory.create('TEST')
        self.avro_message.id = 1111111
        self.avro_message.name = "aaa"
        self.avro_encoded = '\x00\x10\x8e\xd1\x87\x01\x06aaa'

        self.complex_avro_message = self.avro_factory.create('TEST_COMPLEX')
        self.complex_avro_message.id = 1111111
        self.complex_avro_message.name = "aaa"
        self.complex_avro_message.array_complex_field.add()
        self.complex_avro_message.array_complex_field[0].field_1 = "bbb"
        self.complex_avro_message.array_simple_field.add()
        self.complex_avro_message.array_simple_field[0] = "ccc"
        self.complex_avro_message.record_field.field_1 = "ddd"
        self.complex_avro_message.record_field.field_2 = "eee"

        self.complex_avro_encoded = '\x02@\x8e\xd1\x87\x01\x06aaa\x00\x02\x06bbb' \
                                    '\x00\x00\x02\x06ccc\x00\x00\x06ddd\x00\x06eee'

    def test_message_instantiation(self):
        test_message = self.avro_factory.create("TEST")
        self.assertEqual("TEST", test_message.message_type)
        self.assertEqual(TEST_SCHEMA, test_message.schema)

    def test_invalid_message_instantiation(self):
        self.assertRaises(InvalidMessage, self.avro_factory.create, "UNK")
        self.assertRaises(InvalidMessage, self.hl7_factory.create, "UNK")

    def test_simple_message_value_assignment(self, m=None):
        if m is None:
            m = self.avro_factory.create("TEST")
        self.assertEqual(m.id, None)
        self.assertEqual(m.name, None)
        m.id = 1111111
        m.name = "aaa"

        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")

        # with self.assertRaises(AttributeError):
        #     m.unkn = 1

    def test_wrong_schema(self):
        with self.assertRaises(SchemaException):
            self.avro_factory.create("TEST_WRONG")

    def test_complex_message_value_assignment(self):
        m = self.avro_factory.create("TEST_COMPLEX")
        self.test_simple_message_value_assignment(m)

        with self.assertRaises(ValueError):
            m.array_complex_field = [{"field_1": "aaa"}]

        with self.assertRaises(ValueError):
            m.record_field = {"field_1": "aaa"}

        m.array_complex_field.add()
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertIsInstance(m.array_complex_field[0], _Record)
        m.array_complex_field[0].field_1 = "bbb"
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")

        m.array_complex_field.add()
        self.assertEqual(len(m.array_complex_field), 2)
        self.assertIsInstance(m.array_complex_field[1], _Record)
        m.array_complex_field[1].field_1 = "ccc"
        self.assertEqual(m.array_complex_field[1].field_1, "ccc")

        m.array_simple_field.add()
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], None)
        m.array_simple_field[0] = "ccc"
        self.assertEqual(m.array_simple_field[0], "ccc")

        m.array_simple_field.add()
        self.assertEqual(len(m.array_simple_field), 2)
        self.assertEqual(m.array_simple_field[1], None)
        m.array_simple_field[1] = "ddd"
        self.assertEqual(m.array_simple_field[1], "ddd")

        del m.array_complex_field[1]
        self.assertEqual(len(m.array_complex_field), 1)

        del m.array_simple_field[1]
        self.assertEqual(len(m.array_simple_field), 1)

        m.record_field.field_1 = 'aaa'
        self.assertEqual(m.record_field.field_1, 'aaa')

    def test_set_content_message(self):
        content = {
            "id": 1111111,
            "name": "aaa",
            "array_complex_field": [{
                "field_1": "bbb"
            }],
            "array_simple_field": ["ccc"],
            "record_field": {
                "field_1": "ddd"
            }
        }

        # test for the entire message
        m = self.avro_factory.create("TEST_COMPLEX")
        m.set_content(content)

        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], "ccc")
        self.assertEqual(m.record_field.field_1, "ddd")

    def test_set_content_message_wrong(self):
        content = {
            "wrong_id": 1111111,
            "wrong_name": "aaa",
            "wrong_array_complex_field": [{
                "wrong_field_1": "bbb"
            }],
            "wrong_array_simple_field": ["ccc"],
            "wrong_record_field": {
                "wrong_field_1": "ddd"
            }
        }

        # test for the entire message
        m = self.avro_factory.create("TEST_COMPLEX")
        self.assertRaises(AttributeError, m.set_content, content)

        # test for _Record and _Array classes
        self.assertRaises(AttributeError, m.array_complex_field.set_content,
                          content["wrong_array_complex_field"])

        self.assertRaises(AttributeError, m.record_field.set_content,
                          content["wrong_record_field"])

    def test_set_content_array(self):
        content = [{
            "field_1": "bbb"
        }]

        m = self.avro_factory.create("TEST_COMPLEX")

        self.assertRaises(InvalidContent, m.array_complex_field.set_content, 1)  # 1 is not iterable
        m.array_complex_field.set_content(None)
        self.assertEqual(m.array_complex_field.as_obj(), None)
        # self.assertRaises(ValueError, m.array_complex_field.add, "value")
        self.assertRaises(TypeError, m.array_complex_field.__getitem__, 0)
        self.assertRaises(TypeError, m.array_complex_field.__delitem__, 0)
        self.assertRaises(TypeError, m.array_complex_field.__setitem__, 0, "value")
        self.assertRaises(TypeError, len, m.array_complex_field)

        m.array_complex_field.set_content([])
        self.assertEqual(m.array_complex_field.as_obj(), [])
        m.array_complex_field.set_content(content)
        self.assertEqual(m.array_complex_field.as_obj(), [{"field_1": "bbb"}])

    def test_set_content_array_wrong(self):
        content = [{
            "wrong_field_1": "bbb"
        }]

        m = self.avro_factory.create("TEST_COMPLEX")
        self.assertRaises(AttributeError, m.array_complex_field.set_content, content)

    def test_set_content_record(self):
        content = {
            "field_1": "ddd",
            "field_2": "eee"
        }

        m = self.avro_factory.create("TEST_COMPLEX")

        self.assertIsNone(m.record_field.as_obj())
        self.assertRaises(AttributeError, getattr, m.record_field, "field_1")
        self.assertRaises(AttributeError, getattr, m.record_field, "field_2")
        m.record_field.field_1 = 'ddd'
        self.assertEqual(m.record_field.field_1, "ddd")
        self.assertIsNone(m.record_field.field_2)

        m.record_field.set_content(None)
        self.assertIsNone(m.record_field.as_obj())
        self.assertRaises(AttributeError, getattr, m.record_field, "field_1")
        self.assertRaises(AttributeError, getattr, m.record_field, "field_2")

        m.record_field.set_content(content)
        self.assertEqual(m.record_field.field_1, "ddd")
        self.assertEqual(m.record_field.field_2, "eee")

    def test_set_content_record_wrong(self):
        content = {
            "wrong_field_1": "ddd"
        }

        m = self.avro_factory.create("TEST_COMPLEX")

        self.assertRaises(AttributeError, m.record_field.set_content, content)

    def test_retrieve(self):
        m = self.avro_factory.retrieve('\x02@\x8e\xd1\x87\x01\x06aaa\x00\x02\x06bbb'
                                       '\x00\x00\x02\x06ccc\x00\x00\x06ddd\x00\x06eee')
        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], "ccc")
        self.assertEqual(m.record_field.field_1, "ddd")
        self.assertEqual(m.record_field.field_2, "eee")

    def test_avro_serializer(self):
        value = self.avro_message.serialize()
        self.assertEqual(value, self.avro_encoded)

        value = self.complex_avro_message.serialize()
        self.assertEqual(value, self.complex_avro_encoded)