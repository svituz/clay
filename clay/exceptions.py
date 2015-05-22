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

MESSENGER_ERROR_STRINGS = {
    "ERROR_NOQUEUE": "no queue specified for this message",
    "ERROR_NOHANDLER": "no handler defined",
    "ERROR_NOEXCHANGE": "no exchange specified"
}


class SchemaException(Exception):
    pass


class MissingDependency(Exception):
    def __init__(self, requirement_name):
        self.requirement_name = requirement_name

    def __str__(self):
        return "The required package '{}' is not installed. Install it to use the module".format(self.requirement_name)

class InvalidMessage(Exception):
    """
    Raised when the message type does not exists in the catalog
    """
    def __init__(self, message_type):
        self.message_type = message_type

    def __str__(self):
        return "Message %s is not valid" % self.message_type


class InvalidContent(Exception):
    def __str__(self):
        return "Content not valid"


class MessengerError(Exception):
    """
    Generic exception raised when a messenger error occurs
    """
    def __str__(self):
        return "Messenger error"


class MessengerErrorConnectionRefused(MessengerError):
    def __str__(self):
        return "Messenger connection has been refused"


class MessengerErrorNoQueue(MessengerError):
    def __str__(self):
        return "No queue defined for this message"


class MessengerErrorNoHandler(MessengerError):
    def __str__(self):
        return "No handler defined"


class MessengerErrorNoApplicationName(MessengerError):
    def __str__(self):
        return "No application name defined"

# vim:tabstop=4:expandtab
