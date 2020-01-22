import collections.abc
import decimal
from datetime import date, datetime, time
from enum import EnumMeta
from typing import Any, Type, Union
from uuid import UUID

import funcy
from faust.models.fields import DecimalField, FieldDescriptor

from faust_avro.exceptions import UnknownTypeError
from faust_avro.record import Record
from faust_avro.schema import (
    BYTES,
    INT,
    LONG,
    STRING,
    AvroArray,
    AvroEnum,
    AvroField,
    AvroMap,
    AvroRecord,
    AvroUnion,
    DecimalLogicalType,
    LogicalType,
    Schema,
)
from faust_avro.types import datetime_millis, time_millis

LOGICAL_TYPES = (date, time, time_millis, datetime, datetime_millis, UUID)


def parse(registry: Any, model: Any, namespace="") -> Schema:
    """Parse a faust record into an avro schema."""

    origin = getattr(model, "__origin__", None)

    if model in registry:
        schema = registry[model]
    elif isinstance(model, type) and issubclass(model, Record):
        schema = parse_record(registry, model, namespace)
    elif model in LOGICAL_TYPES:
        schema = parse_logical(registry, model, namespace)
    elif isinstance(model, EnumMeta):
        schema = parse_enum(registry, model, namespace)
    elif origin == Union:
        schema = parse_union(registry, model, namespace)
    elif origin and issubclass(origin, collections.abc.Sequence):
        schema = parse_array(registry, model, namespace)
    elif origin and issubclass(origin, collections.abc.Mapping):
        schema = parse_mapping(registry, model, namespace)
    else:
        raise UnknownTypeError(f"No avro type known for {model}.")

    return schema


def parse_record(registry: Any, model: Type[Record], namespace: str) -> Schema:
    """Parse a faust record into an avro schema."""
    record = registry.add(
        AvroRecord(
            name=model._avro_name,
            aliases=model._avro_aliases,
            python_type=model,
            doc=model.__doc__,
        )
    )
    record.fields = [
        parse_field(registry, getattr(model, field), namespace)
        for field in model._options.fields
    ]
    return record


def parse_field(registry: Any, model: FieldDescriptor, namespace: str) -> Schema:
    """Parse a faust record's fields into avro fields."""
    if isinstance(model, DecimalField):
        if model.max_digits is not None or model.max_decimal_places is not None:
            precision = (model.max_digits or 0) + (model.max_decimal_places or 0)
        else:
            precision = decimal.getcontext().prec
        scale = model.max_decimal_places
        schema = DecimalLogicalType(
            schema=BYTES, logical_type="decimal", precision=precision, scale=scale
        )
    else:
        schema = parse(registry, model.type, namespace)

    if model.required:
        return AvroField(model.field, schema)
    else:
        return AvroField(model.field, schema, None, default=model.default)


def parse_enum(registry: Any, model: EnumMeta, namespace: str) -> Schema:
    """Parse a python enum into an avro enum.

    Since avro enums are named types, the name of the python enum will
    be its fully qualified module path and name.
    """
    return registry.add(
        AvroEnum(
            name=f"{model.__module__}.{model.__name__}",
            aliases=[model.__name__],
            doc=model.__doc__,
            symbols=list(model.__members__.keys()),
        )
    )


def parse_union(registry: Any, model: Any, namespace: str) -> Schema:
    """Parse a python type hint union into an avro union.

    Note: due to how avro works with defaults, if None is part of a union,
    this method will force it to be the first item in the avro union, so
    that it can be used as a default. This is considered `best practices
    <https://avro.apache.org/docs/current/spec.html#Unions>_`.
    """
    args = model.__args__
    if type(None) in args:
        args = funcy.distinct([type(None), *args])
    return AvroUnion(schemas=[parse(registry, schema, namespace) for schema in args])


def parse_array(registry: Any, model: Any, namespace: str) -> Schema:
    """Parse a python sequence into an avro array type."""
    return AvroArray(items=parse(registry, model.__args__[0], namespace))


def parse_mapping(registry: Any, model: Any, namespace: str) -> Schema:
    """Parse a python mapping into an avro map type.

    :raises:
        TypeError: If the python mapping uses non-string keys. Avro only supports string keys in mappings.
    """
    key, value = model.__args__
    # Avro maps require keys to be strings.
    if not issubclass(key, str):
        raise TypeError(f"{model} does not have string-like keys.")
    return AvroMap(values=parse(registry, value, namespace))


LOGICAL = {
    date: LogicalType(logical_type="date", schema=INT),
    time: LogicalType(logical_type="time-micros", schema=LONG),
    time_millis: LogicalType(logical_type="time-millis", schema=INT),
    datetime: LogicalType(logical_type="timestamp-micros", schema=LONG),
    datetime_millis: LogicalType(logical_type="timestamp-millis", schema=LONG),
    UUID: LogicalType(logical_type="uuid", schema=STRING),
}


def parse_logical(registry: Any, model: Any, namespace: str) -> Schema:
    """Parse a python type which maps to an avro logical type."""
    return LOGICAL[model]
