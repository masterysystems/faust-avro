import collections.abc
from typing import Any, Dict, Iterable, Optional

from faust_avro.exceptions import UnknownTypeError
from faust_avro.schema import (
    AvroArray,
    AvroEnum,
    AvroField,
    AvroFixed,
    AvroMap,
    AvroNested,
    AvroRecord,
    AvroSchemaT,
    AvroUnion,
    LogicalType,
    Schema,
)


def parse(registry: Any, schema: AvroSchemaT) -> Schema:
    """Parse a json-parsed avro schema into intermediate form.

    Ref: https://avro.apache.org/docs/current/spec.html#schemas"""
    if isinstance(schema, str):
        if schema in registry:
            return registry[schema]
    elif isinstance(schema, collections.abc.Sequence):
        return AvroUnion(schemas=[parse(registry, s) for s in schema])
    elif isinstance(schema, collections.abc.Mapping):
        return parse_logical_type(registry, **schema)

    raise UnknownTypeError(schema)


def parse_logical_type(registry: Any, **kwargs: Any) -> Schema:
    """Parse a possible logical type in addition to the complex schema."""
    logical_type = kwargs.pop("logicalType", None)
    schema = parse_complex(registry, **kwargs)
    if logical_type is not None:
        schema = LogicalType(schema=schema, logical_type=logical_type)
    return schema


def parse_record_field(registry: Any, *, type, **kwargs: Any) -> AvroField:
    """Helper function to parse the type of a record field."""
    return AvroField(type=parse(registry, type), **kwargs)


def parse_complex(
    registry: Any,
    *,
    type: AvroSchemaT,
    fields: Iterable[Dict[str, AvroSchemaT]] = (),
    items: Optional[AvroSchemaT] = None,
    values: Optional[AvroSchemaT] = None,
    **kwargs: Any,
) -> Schema:
    """Helper function to parse one of the avro complex record types.

    Ref: https://avro.apache.org/docs/current/spec.html#schema_complex"""
    if type == "record":
        # Define the record early, so that it can reference itself by name
        # for recursive definitions (eg, LinkedList).
        schema = registry.add(AvroRecord(**kwargs))
        schema.fields = [parse_record_field(registry, **f) for f in fields]
    elif type == "enum":
        schema = registry.add(AvroEnum(**kwargs))
    elif type == "array":
        schema = AvroArray(items=parse(registry, items))
    elif type == "map":
        schema = AvroMap(values=parse(registry, values))
    elif type == "fixed":
        schema = registry.add(AvroFixed(**kwargs))
    elif isinstance(type, dict):
        # For the outer dict in: `{"type": {"type": whatever}}`
        schema = AvroNested(schema=parse(registry, type))
    elif type in registry:
        # For the inner dict in: `{"type": {"type": whatever}}`
        schema = AvroNested(schema=registry[type])
    else:
        raise UnknownTypeError(type)

    return schema
