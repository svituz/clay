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

from clay.exceptions import SchemaException

__author__ = "Massimo Gaggero, Vittorio Meloni"
__author_email__ = "<massimo.gaggero@crs4.it>, <vittorio.meloni@crs4.it>"
__url__ = "https://github.com/crs4/clay"

MESSAGE_FACTORIES = {}
CATALOGS = {}
NAMED_CATALOGS = {}


class MessageFactoryMetaclass(type):
    def __call__(cls, serializer, catalog):
        try:
            _factory = MESSAGE_FACTORIES[(serializer, catalog['name'])]
        except KeyError:
            _factory = type.__call__(cls, serializer, catalog)
            MESSAGE_FACTORIES[(serializer, catalog['name'])] = _factory
        return _factory


def add_catalog(catalog):
    CATALOGS[catalog["name"]] = catalog
    NAMED_CATALOGS[catalog["name"]] = dict((v["name"], (k, v)) for (k, v) in catalog.viewitems()
                                           if isinstance(k, int))

# def schema_from_name(schema_name):
#     """
#     It searches the schema with the name :attr:`schema_name` in all the catalog.
#     It returns the corresponding id of the schema and the schema itself
#
#     :param schema_name: The name of the schema to search
#     :return: a tuple wit the ID of the schema and the schema itself
#     """
#     for key, catalog in NAMED_CATALOGS.items():
#         try:
#             return catalog[schema_name]
#         except KeyError:
#             continue
#
#     raise SchemaException("Schema '%s' does not exist" % schema_name)

def schema_from_name(schema_name, schema_catalog):
    """
    It searches the schema with the name :attr:`schema_name` in all the catalog.
    It returns the corresponding id of the schema and the schema itself

    :param schema_name: The name of the schema to search
    :return: a tuple wit the ID of the schema and the schema itself
    """
    named_catalog = dict((s["name"], (i, s)) for (i, s) in schema_catalog.viewitems()
                         if isinstance(i, int))
    try:
        return named_catalog[schema_name]
    except KeyError:
        raise SchemaException("Schema '%s' does not exist" % schema_name)


def schema_from_id(schema_id, schema_domain):
    """
    It searches the schema with the id :attr:`schema_id` in the catalog
    of the :attr:`schema_domain`.
    It returns the corresponding id of the schema and the schema itself

    :param schema_id: The ID of the schema to search
    :param schema_domain: The Domain of the schema to search
    :return: the schema
    """
    try:
        return CATALOGS[schema_domain][schema_id]
    except KeyError:
        raise SchemaException("Schema id '%d' does not exist in '%s' catalog" % (schema_id, schema_domain))

# vim:tabstop=4:expandtab