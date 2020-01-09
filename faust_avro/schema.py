"""
Intermediate form schema definitions.

These get constructed from either a json-parsed avro schema, or from a python
dataclass.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from importlib import import_module
from typing import Any, Iterable, List, Optional, Set

from faust_avro.types import float32, int32

__all__ = [
    # Types
    "AvroSchemaT",
    "VisitedT",
    # Classes
    "AvroRecord",
    "AvroEnum",
    "AvroArray",
    "AvroMap",
    "AvroFixed",
    "AvroUnion",
    "AvroNested",
    "AvroField",
    "NamedSchema",
    "Primitive",
    "Schema",
    # Constants
    "PRIMITIVES",
]


MISSING = object()


# https://github.com/python/mypy/issues/7069
# AvroSchemaT = Union[str, List["AvroSchemaT"], Dict[str, "AvroSchemaT"]]
AvroSchemaT = Any
VisitedT = Set[str]


@dataclass  # type: ignore
# https://github.com/python/mypy/issues/5374
class Schema(ABC):
    def _add_fields(self, *fields, **schema) -> AvroSchemaT:
        for f in fields:
            value = getattr(self, f)
            if value and value != MISSING:
                schema[f] = value
        return {k: v for k, v in schema.items() if v != MISSING}

    @staticmethod
    def _import_class(path: str) -> type:
        """Extract a single class/object from within a module."""
        try:
            module, name = path.rsplit(".", 1)
            return getattr(import_module(module), name)
        except Exception as e:
            raise ImportError(f"{path} not found.") from e

    @abstractmethod
    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        """The implementation of intermediate->avro."""
        # VisitedT is used to prevent infinite recursion. The first time a
        # schema is getting dumped by _to_avro, it should be dumped in full
        # and then visited should be updated to include that schema by name,
        # so that if it is seen again it is dumped as a named type.

    def to_avro(self) -> AvroSchemaT:
        """Return an avro str/list/dict schema for this intermediate schema."""
        visited: VisitedT = set()
        return self._to_avro(visited)


@dataclass
class LogicalType(Schema):
    """A generic LogicalType wrapping a normal avro type."""

    schema: Schema
    logical_type: str

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        schema = self.schema._to_avro(visited)
        if isinstance(schema, str):
            # Primitives return bare strings, so turn those into a dict
            schema = dict(type=schema)
        schema["logicalType"] = self.logical_type
        return schema


@dataclass
class DecimalLogicalType(LogicalType):
    """A LogicalType that supports the decimal precision and scale arguments."""

    precision: int
    scale: Optional[int] = None

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        schema = super()._to_avro(visited)
        schema["precision"] = self.precision
        if self.scale is not None:
            schema["scale"] = self.scale
        return schema


@dataclass
class Primitive(Schema):
    """Primitive avro types: https://avro.apache.org/docs/current/spec.html#schema_primitive"""

    name: str
    python_type: Optional[
        type
    ]  # Optional allows None, which is "weird" in python typing

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        return self.name


NULL = Primitive("null", type(None))
BOOL = Primitive("boolean", bool)
INT = Primitive("int", int32)
LONG = Primitive("long", int)
FLOAT = Primitive("float", float32)
DOUBLE = Primitive("double", float)
BYTES = Primitive("bytes", bytes)
STRING = Primitive("string", str)

PRIMITIVES: List[Primitive] = [NULL, BOOL, INT, LONG, FLOAT, DOUBLE, BYTES, STRING]


@dataclass
class NamedSchema(Schema):
    """Used for the named avro schema types."""

    name: str
    namespace: Optional[str] = ""
    aliases: Iterable[str] = field(default_factory=list)

    python_type: Optional[type] = field(default=None, compare=False)

    def __post_init__(self) -> None:
        try:
            self.python_type = self._import_class(self.name)
        except ImportError:
            pass

    def _to_avro(
        self, visited: VisitedT, *fields: str, **extras: AvroSchemaT
    ) -> AvroSchemaT:
        if self.name in visited:
            return self.name
        else:
            visited.add(self.name)
            return dict(
                **extras, **self._add_fields("name", "namespace", "aliases", *fields)
            )


class Ordering(Enum):
    """How a field within a record impacts sorting multiple records"""

    ASCENDING = "ascending"
    DESCENDING = "descending"
    IGNORE = "ignore"


@dataclass
class AvroField(Schema):
    """A single field within an avro Record schema"""

    name: str
    type: Any
    doc: Optional[str] = None
    aliases: Iterable[str] = field(default_factory=list)

    # Can't use None, because that's a valid default
    default: Optional[Any] = MISSING

    # Must be None so we don't add this to the schema if unspecified
    order: Optional[Ordering] = None

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        return self._add_fields(
            "name",
            "doc",
            "order",
            "aliases",
            type=self.type._to_avro(visited),
            default=self.default,
        )


@dataclass
class AvroRecord(NamedSchema):
    """https://avro.apache.org/docs/current/spec.html#schema_record"""

    doc: Optional[str] = None
    fields: Iterable[AvroField] = field(default_factory=list)

    schema_id: Optional[int] = None

    def _to_avro(
        self, visited: VisitedT, *fields: str, **extras: AvroSchemaT
    ) -> AvroSchemaT:
        # Delay trying to flatten the fields, because the super() call here
        # adds self to visited, so that when we later flatten fields, any
        # references to this record itself will come out as a named type.
        result = super()._to_avro(visited, "doc", *fields, type="record", **extras)
        if not isinstance(result, str):
            result["fields"] = [field._to_avro(visited) for field in self.fields]
        return result


@dataclass
class AvroEnum(NamedSchema):
    """https://avro.apache.org/docs/current/spec.html#Enums"""

    doc: Optional[str] = None
    symbols: Iterable[str] = field(default_factory=list)
    default: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.python_type is None:
            self.python_type = Enum(self.name, " ".join(self.symbols))

    def _to_avro(
        self, visited: VisitedT, *fields: str, **extras: AvroSchemaT
    ) -> AvroSchemaT:
        return super()._to_avro(
            visited,
            "doc",
            "default",
            *fields,
            type="enum",
            symbols=list(self.symbols),
            **extras,
        )


@dataclass
class AvroArray(Schema):
    """https://avro.apache.org/docs/current/spec.html#Arrays"""

    items: Schema

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        return dict(type="array", items=self.items._to_avro(visited))


@dataclass
class AvroMap(Schema):
    """https://avro.apache.org/docs/current/spec.html#Maps"""

    values: Schema

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        return dict(type="map", values=self.values._to_avro(visited))


@dataclass
class AvroFixed(NamedSchema):
    """https://avro.apache.org/docs/current/spec.html#Fixed"""

    size: int = 0

    def _to_avro(
        self, visited: VisitedT, *fields: str, **extras: AvroSchemaT
    ) -> AvroSchemaT:
        return super()._to_avro(
            visited, *fields, type="fixed", size=self.size, **extras
        )


@dataclass
class AvroUnion(Schema):
    """https://avro.apache.org/docs/current/spec.html#Unions"""

    schemas: Iterable[Schema]

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        return [schema._to_avro(visited) for schema in self.schemas]


@dataclass
class AvroNested(Schema):
    """
    An arbitrary nesting, where the schema used the second form of schema declaration from
    https://avro.apache.org/docs/current/spec.html#schemas to "nest" a schema with an extra dict.

    Example:
        {"type": {"type": "str"}}
    As opposed to the simpler:
        {"type": "str"}
    Or even just:
        "str"
    """

    schema: Schema

    def _to_avro(self, visited: VisitedT) -> AvroSchemaT:
        return dict(type=self.schema._to_avro(visited))
