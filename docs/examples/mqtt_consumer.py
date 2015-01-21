#!/usr/bin/env python

from clay.factory import MessageFactory
from clay.messenger import MQTTBroker
from clay.serializer import AvroSerializer

from example_catalog import SINGLE_EXAMPLE_CATALOG

mf = MessageFactory(AvroSerializer, SINGLE_EXAMPLE_CATALOG)

def my_handler(body, message_type):
    try:
        print message_type, mf.retrieve(body).fields
    except Exception as ex:
        print ex

brk = MQTTBroker()
brk.set_credentials('clay', 'clay')
brk.set_queue('EXAMPLES', durable=True, response=False)
brk.handler = my_handler
brk.run()
