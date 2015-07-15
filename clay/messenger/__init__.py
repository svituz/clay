import sys
import logging
from .. import CustomLoader
from ..exceptions import MissingDependency


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
try:
    from .amqp_messenger import AMQPMessenger, AMQPReceiver
except MissingDependency as md:
    pass

try:
    from .mqtt_messenger import MQTTMessenger, MQTTReceiver
except MissingDependency:
    pass

try:
    from .kafka_messenger import KafkaMessenger, KafkaError
except MissingDependency:
    pass

class _MessengerLoader(CustomLoader):

    DEPENDENCIES = {
        "clay.messenger.AMQPMessenger": "pika",
        "clay.messenger.AMQPReceiver": "pika",
        "clay.messenger.MQTTMessenger": "paho",
        "clay.messenger.MQTTReceiver": "paho",
        "clay.messenger.KafkaMessenger": "kafka",
    }

sys.meta_path.append(_MessengerLoader())

# vim:tabstop=4:expandtab
