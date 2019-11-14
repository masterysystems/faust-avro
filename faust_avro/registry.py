from typing import Any

from faust_avro.exceptions import SchemaAlreadyDefinedError, SchemaException
from faust_avro.parsers.avro import parse
from faust_avro.schema import PRIMITIVES, AvroSchemaT, NamedSchema, Schema


class Registry(dict):
    """A write-once schema registry."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for primitive in reversed(PRIMITIVES):
            try:
                self.add(primitive)
            except SchemaAlreadyDefinedError:
                # Expected for the two ints and two floats
                pass

    def __setitem__(self, key: Any, value: Schema) -> None:
        """Write-once semantics, ignoring no-op writes."""

        if key in self and self[key] != value:
            raise SchemaAlreadyDefinedError(key)
        else:
            super().__setitem__(key, value)

    def add(self, schema: NamedSchema) -> Schema:
        """Add a schema and all its aliases and type to the registry."""

        for name in [schema.name, *getattr(schema, "aliases", ())]:
            self[name] = schema

        if schema.python_type is not None:
            self[schema.python_type] = schema

        return schema

    def parse(self, schema: AvroSchemaT) -> Schema:
        """Parse a python type or avro json schema definition into intermediate form."""

        previous = self.copy()
        try:
            return parse(self, schema)
        except SchemaException:
            self.clear()
            self.update(previous)
            raise
