import sys
import logging
from .. import _CustomLoader
from ..exceptions import MissingDependency

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

class _MessengerLoader(_CustomLoader):

    DEPENDENCIES = {
        "clay.messenger.AMQPMessenger": "pika",
        "clay.messenger.AMQPReceiver": "pika",
        "clay.messenger.MQTTMessenger": "paho",
        "clay.messenger.MQTTReceiver": "paho",
        "clay.messenger.KafkaMessenger": "kafka",
    }

    # def find_module(self, name, path=None):
    #     if name in self.DEPENDENCIES:
    #         self.path = path
    #         return self
    #     return None
    #
    # def load_module(self, name):
    #     if name in sys.modules:
    #         return sys.modules[name]
    #
    #     try:
    #         module_info = imp.find_module(name.split(".")[-1], self.path)
    #         module = imp.load_module(name, *module_info)
    #     except ImportError:
    #         raise MissingDependency(self.DEPENDENCIES[name])
    #     else:
    #         sys.modules[name] = module
    #
    #     return module

sys.meta_path.append(_MessengerLoader())

# vim:tabstop=4:expandtab
