import logging

logging.basicConfig()


class Messenger(object):
    def __init__(self):
        pass

    def send(self, message):
        pass


class Dummy(Messenger):
    def __init__(self):
        pass

    def send(self, serializer):
        print("Dummy using messenger", serializer.serialize())

# Imports the other Messengers
from .amqp import AMQPMessenger, AMQPReceiver
from .mqtt import MQTTMessenger, MQTTReceiver

# vim:tabstop=4:expandtab
