__all__ = ["SchemaException", "UnknownTypeError", "SchemaAlreadyDefinedError"]


class SchemaException(Exception):
    """Parent exception for errors related to avro schemas."""


class CodecException(SchemaException):
    """There were problems with serialization/deserialization."""


class SchemaAlreadyDefinedError(SchemaException):
    """A schema of the given name is already defined in this registry."""


class UnknownTypeError(SchemaException):
    """The given type does not match any known or user-defined avro type."""
