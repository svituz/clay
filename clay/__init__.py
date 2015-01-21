from clay.exceptions import SchemaException

MESSAGE_FACTORIES = {}
CATALOGS = {}
NAMED_CATALOGS = {}


class MessageFactoryMetaclass(type):
    def __call__(cls, serializer, catalog):
        try:
            _factory = MESSAGE_FACTORIES[(serializer, catalog['name'])]
        except KeyError as ke:
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