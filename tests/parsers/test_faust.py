from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union
from uuid import UUID

from faust.models.fields import DecimalField

import pytest
from assertpy import assert_that
from faust_avro import Record, UnknownTypeError
from faust_avro.parsers.faust import parse
from faust_avro.schema import (
    BOOL,
    BYTES,
    DOUBLE,
    FLOAT,
    INT,
    LONG,
    NULL,
    STRING,
    AvroArray,
    AvroEnum,
    AvroField,
    AvroMap,
    AvroRecord,
    AvroUnion,
    DecimalLogicalType,
    LogicalType,
)
from faust_avro.types import datetime_millis, float32, int32, time_millis


class Colors(Enum):
    red = 0
    green = 1
    blue = 2


def test_faust(registry):
    class Nested(Record, avro_name="net.mastery.Nested"):
        nested: str

    class Faust(Record, avro_name="net.mastery.Faust"):
        """My docstring"""

        # Primitive types
        boolean: bool
        integer: int32
        longint: int
        floating_pt: float32
        double: float
        byte_string: bytes
        text_string: str
        # Complex types
        color: Colors
        words: List[str]
        counts: Dict[str, int]
        numeric: Union[int, float]
        # Logical Types
        many_digits: Decimal
        usd: Decimal = DecimalField(max_digits=20, max_decimal_places=2)
        calendar_date: date
        daily_time: time
        daily_time_millis: time_millis
        timestamp: datetime
        timestamp_millis: datetime_millis
        uuid: UUID
        # Pythonic types
        optional: Optional[str]  # Faust auto-defaults this to None.
        default: str = "a default"
        # Nested
        nested: Optional[Nested] = None
        # Recursive
        recursive: Optional["Faust"] = None

    # Parse the class...
    gen = Faust.to_avro(registry)

    assert_that(registry).contains(
        "net.mastery.Faust", "Faust", "net.mastery.Nested", "Nested"
    )
    nested = AvroRecord(
        name="net.mastery.Nested",
        aliases=["Nested"],
        fields=[AvroField(name="nested", type=STRING)],
    )
    expected = AvroRecord(
        name="net.mastery.Faust",
        doc="My docstring",
        aliases=["Faust"],
        fields=[
            # Primitives
            AvroField(name="boolean", type=BOOL),
            AvroField(name="integer", type=INT),
            AvroField(name="longint", type=LONG),
            AvroField(name="floating_pt", type=FLOAT),
            AvroField(name="double", type=DOUBLE),
            AvroField(name="byte_string", type=BYTES),
            AvroField(name="text_string", type=STRING),
            # Complex
            AvroField(
                name="color",
                type=AvroEnum(
                    "test_faust.Colors",
                    aliases=["Colors"],
                    doc="An enumeration.",
                    symbols=["red", "green", "blue"],
                ),
            ),
            AvroField(name="words", type=AvroArray(STRING)),
            AvroField(name="counts", type=AvroMap(LONG)),
            AvroField(name="numeric", type=AvroUnion([LONG, DOUBLE])),
            # Logical Types
            AvroField(
                name="many_digits",
                type=DecimalLogicalType(
                    schema=BYTES, logical_type="decimal", precision=28
                ),
            ),
            AvroField(
                name="usd",
                type=DecimalLogicalType(
                    schema=BYTES, logical_type="decimal", precision=22, scale=2
                ),
            ),
            AvroField(
                name="calendar_date", type=LogicalType(logical_type="date", schema=INT)
            ),
            AvroField(
                name="daily_time",
                type=LogicalType(logical_type="time-micros", schema=LONG),
            ),
            AvroField(
                name="daily_time_millis",
                type=LogicalType(logical_type="time-millis", schema=INT),
            ),
            AvroField(
                name="timestamp",
                type=LogicalType(logical_type="timestamp-micros", schema=LONG),
            ),
            AvroField(
                name="timestamp_millis",
                type=LogicalType(logical_type="timestamp-millis", schema=LONG),
            ),
            AvroField(
                name="uuid", type=LogicalType(logical_type="uuid", schema=STRING)
            ),
            # Pythonic
            AvroField(name="optional", type=AvroUnion([NULL, STRING]), default=None),
            AvroField(name="default", type=STRING, default="a default"),
            AvroField(name="nested", type=AvroUnion([NULL, nested]), default=None),
            # ... recursive entry is added below
        ],
        python_type=Faust,
    )
    expected.fields.append(
        AvroField(name="recursive", type=AvroUnion([NULL, expected]), default=None)
    )
    assert_that(gen).is_equal_to(expected.to_avro())


@pytest.mark.parametrize(
    "exception,python", [(TypeError, Dict[int, int]), (UnknownTypeError, Path)]
)
def test_faust_garbage(registry, exception, python):
    with pytest.raises(exception):
        parse(registry, python)
