from abc import abstractmethod


class Dialect(object):
    """
    Enum class for translator dialects
    (e.g. Postgres, MySQL, ... or even non-database stuff)
    """
    POSTGRES = 1,
    STANDARD = 99


class Datatype(object):
    """
    Our supported datatypes.
    """
    STRING = "String"
    NUMERIC = "Numeric"
    DOUBLE = "Double"
    DATETIME = "DateTime"
    DATE = "Date"
    TIME = "Time"
    BOOLEAN = "Boolean"
    CURRENCY = "Currency"
    HASH = "Hash"

    __LOOKUP = {
        "String": STRING,
        "Str": STRING,
        "Numeric": NUMERIC,
        "Integer": NUMERIC,
        "Int": NUMERIC,
        "Double": DOUBLE,
        "Decimal": DOUBLE,
        "Float": DOUBLE,
        "DateTime": DATETIME,
        "Timestamp": DATETIME,
        "Date": DATE,
        "Time": TIME,
        "Boolean": BOOLEAN,
        "Bool": BOOLEAN,
        "Currency": CURRENCY,
        "Hash": HASH
    }

    def __init__(self, type):
        """
        Constructor.
        :param type: candidate to check for a valid datatype.
        """
        self.type = Datatype.__resolve_datatype(type)

    def translate(self, dialect=None):
        """
        Using the specified dialect a translator is resolved to
        translate this datatype to a target datatype.
        Example:
            Datatype.parse('String').translate(dialect=Dialect.POSTGRES)
        :param dialect: Dialect to use (e.g. Postgres).
        :return: Returns a translator.
        """
        return get_translator_clazz(dialect)(self)

    def __str__(self):
        """
        The informal representation of this class.
        :return: Returns the informal representation of this instance.
        """
        return self.type

    def __repr__(self):
        """
        The formal representation of this class. You could put the
        output to eval to create the exact same datatype.
        :return: Returns the formal representation of this instance.
        """
        return "Datatype(type='{}')".format(self.type)

    @staticmethod
    def __resolve_datatype(candidate: str):
        """
        Checks if the given candidate is a valid / supported datatype.
        Raises a ValueError if the candidate is not valid.
        :param candidate: Candidate to check.
        :return: Returns the official system representation of the candidate.
        """
        for key, value in Datatype.__LOOKUP.items():
            if key.lower() == candidate.lower():
                return value

        raise ValueError(
            "Argument 'candidate' is not a supported datatype. "
            "Value is '{}'. Possible types are: '{}'".format(
                str(candidate),
                ", ".join(Datatype.__LOOKUP.keys())
            )
        )

    @staticmethod
    def parse(candidate, fail_on_error=True):
        """
        Parses the given candidate and checks if it is a supported datatype.
        A ValueError is thrown when the cancidate is not supported.
        :param candidate: Candidate to check for compatibility.
        :param fail_on_error: If True this will suppress the exception
                              and return Datatype.STRING instead
        :return: Returns an instance of Datatype
                (with the correct datatype representation)
        """
        try:
            return Datatype(candidate)
        except ValueError:
            if fail_on_error:
                return Datatype(Datatype.STRING)
            else:
                raise


def get_translator_clazz(dialect=None):
    """
    Factory method to return an applicable translator for the given dialect.
    Please check the Dialect class enum for available dialects.
    If no dialect is specified a default one will be returned
    (and that is up to the factory -> so be careful :P).
    :param dialect: Dialect you translator should support.
    :return: Returns a translator that supports the specified dialect.
    """
    available_dialects = {
        Dialect.POSTGRES: PostgresTranslator,
        Dialect.STANDARD: PostgresTranslator
    }

    # If none dialect is provided use a standard
    if dialect is None:
        dialect = Dialect.STANDARD

    if dialect in available_dialects:
        return available_dialects[dialect]

    raise ValueError("Provided dialect is unknown. Abort...")


class Translator(object):
    """
    A Translator 'translates' datatypes between the internal
    datatype namespace (see class Datatype) and an
    external dialect (e.g. Postgres database).
    """

    def __init__(self, datatype: Datatype):
        """
        Constructor.
        :param datatype: The datatype the Translator should care for.
        """
        self.datatype = datatype

    @abstractmethod
    def hash_function(self, values=None):
        """
        Returns the hash function as a string for use in sql templates
        that the given dialect supports.
        If the argument 'values' is specified, the hash function
        is directly applied to the given values.
        If the argument 'values' is not specified just a hash function
        with the placeholder 'VALUES' is returned.
        Do h = instance.hash_function().format(VALUES='my_value')
        :param values: Optional values to apply the hash function to.
        :return: A String with the correct use of the dialects hash function.
        """
        raise NotImplementedError(
            "You have to implement this method in a child class, dude!"
        )

    @abstractmethod
    def dialect_datatype(self):
        """
        Returns a datatype that is supported by the dialect.
        For example the result can be used for table creations.
        :return:
        """
        raise NotImplementedError(
            "You have to implement this method in a child class, dude!"
        )

    @abstractmethod
    def default_value(self):
        """
        Returns a default value for the given datatype (constructor argument)
        that is supported / accepted
        by the dialect.
        :return:
        """
        raise NotImplementedError(
            "You have to implement this method in a child class, dude!"
        )

    @abstractmethod
    def cast(self, target_type):
        """
        Prepares a cast statement that is supported / accepted by the dialect.
        Do
            c = instance.cast().format(VALUE='my_value')
        :return:
        """
        raise NotImplementedError(
            "You have to implement this method in a child class, dude!"
        )


class PostgresTranslator(Translator):
    """
    The translator 'understands' the postgres dialect.
    """
    # Internal mapping from internal namespace to postgres namespace
    MAPPING = {
        Datatype.BOOLEAN: {
            "type": "boolean",
            "lookups": ['boolean', 'bool'],
            "default": "False",
            "cast": "{VALUE}::boolean"
        },
        Datatype.CURRENCY: {
            "type": "money",
            "lookups": ["money"],
            "default": "0.0",
            "cast": "{VALUE}::currency"
        },
        Datatype.DATETIME: {
            "type": "timestamp with time zone",
            "lookups": [
                'timestamp with time zone',
                'timestamp',
                'timestamp without time zone'
            ],
            "default": "'1970-01-01 00:00:00.000'",
            "cast": "to_timestamp({VALUE}, 'YYYY-MM-DD HH24:MI:SS.US')"
        },
        Datatype.DOUBLE: {
            "type": "double precision",
            "lookups": [
                'double precision',
                'double',
                'float',
                'real',
                'numeric',
                'decimal'
            ],
            "default": "0.0",
            "cast": "CAST({VALUE} AS double precision)"
        },
        Datatype.NUMERIC: {
            "type": "bigint",
            "lookups": [
                'bigint',
                'integer',
                'smallint',
                'int'
            ],
            "default": "0",
            "cast": "{VALUE}::bigint"
        },
        Datatype.STRING: {
            "type": "text",
            "lookups": [
                'character varying',
                'text',
                'character',
                'char'
            ],
            "default": "''",
            "cast": "{VALUE}::text"
        },
        Datatype.HASH: {
            "type": "char(32)",
            "lookups": [],
            "default": "''",
            "cast": "{VALUE}::text"
        },
        Datatype.DATE: {
            "type": "date",
            "lookups": ["date"],
            "default": "'1970-01-01'",
            "cast": "{VALUE}::date"
        },
        Datatype.TIME: {
            "type": "time with time zone",
            "lookups": [
                "time",
                "time with time zone",
                "time without time zone"
            ],
            "default": "'00:00:00.000'",
            "cast": "{VALUE}::::time with time zone"
        }


    }

    @staticmethod
    def __invert():
        def _put(lookup, key, datatype):
            if key in lookup and datatype != lookup[key]:
                raise KeyError(
                    "It looks like that you have the same synonym for "
                    "an environment for different environments. "
                    "This is a configuration error. Fix it!"
                )
            lookup[key] = datatype

        reversed_lookup = {}
        for k, v in PostgresTranslator.MAPPING.items():
            for elem in v['lookups']:
                _put(reversed_lookup, elem, k)

        return reversed_lookup

    def __init__(self, datatype):
        """
        Constructor.
        :param datatype: Datatype instance.
        """
        super(PostgresTranslator, self).__init__(datatype)
        if self.datatype.type not in PostgresTranslator.MAPPING:
            raise ValueError("Datatype is not supported")

    def hash_function(self, values=None):
        """
        Returns the postgres hash function as a string for use in sql templates
        * If the argument 'values' is specified,
        the hash function is directly applied to the given values.
        * If the argument 'values' is not specified just a hash function
        with the placeholder 'VALUES' is returned.
        Do
            h = instance.hash_function().format(VALUES='my_value')
            # 'md5(my_value)'

        In addition: If you put in a list of tuples or a single tuple,
        it is assumed that the first element is the
        actual value; the second element represents the datatype
        (in the systems internal speech -> see Datatype class).
        If you provide this correctly the resulting hash function will
        handle NULL-Values correctly.
        Hint: NULL Values will lead to NULL hash values when the hash
              function is applied. So we schould avoid this.
        Example
            t = PostgresTranslator(Datatype.parse(Datatype.STRING))
            t.hash_function([('a', Datatype.STRING),('b', Datatype.NUMERIC)])
            # 'md5(COALESCE(a, '') || COALESCE(b, 0))'
        :param values: Optional values to apply the hash function to.
        :return: A String with the correct use of the dialects hash function.
        """
        if values is None:
            return "md5({VALUES}::text)"

        if not isinstance(values, list):
            values = [values]

        if len(values) == 0:
            return "md5({VALUES}::text)"

        input_vals = []
        for value in values:
            if isinstance(value, tuple) and len(value) == 2:
                dt = Datatype.parse(value[1])
                dv = dt.translate(Dialect.POSTGRES).default_value()
                input_vals.append(
                    "COALESCE({}, {})::text".format(value[0], dv)
                )
            else:
                # No datatype provided just the value
                input_vals.append(str(value) + "::text")

        return "md5({})".format(" || '#' || ".join(input_vals))

    def dialect_datatype(self):
        """
        Returns a datatype that is supported by postgres.
        For example the result can be used for table creations.
        :return:
        """
        return PostgresTranslator.MAPPING[self.datatype.type]['type']

    def default_value(self):
        """
        Returns a default value for the given datatype
        (constructor argument) that is supported / accepted
        by postgres.
        :return:
        """
        return PostgresTranslator.MAPPING[self.datatype.type]['default']

    def cast(self, target_type: Datatype):
        """
        Prepares a cast statement that is supported / accepted by the dialect.
        Do
            c = instance.cast().format(VALUE='my_value')
        :return:
        """
        return PostgresTranslator.MAPPING[target_type.type]['cast']

    @staticmethod
    def reverse_lookup(source_type):
        """
        In case you have a postgeres data type and you need the
        Datatype representation...
        PostgresTranslator.reverse_lookup('bigint') => Datatype.NUMERIC
        :param source_type: Postgres data type
        :return: Datatype
        """
        import re
        inverse = PostgresTranslator.__invert()
        source_type = re.sub('[^a-zA-Z ]', '', source_type)
        for k, v in inverse.items():
            if k.lower() == source_type.lower():
                return v
        return Datatype.STRING
