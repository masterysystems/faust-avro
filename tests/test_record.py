from typing import List, Union

import pytest
from assertpy import assert_that
from faust_avro import Record
from faust_avro import context as ctx
from faust_avro.serializers import Codec


class Inner(Record, coerce=True):
    name: str


class Outer(Record, coerce=True):
    index: int
    value: Inner


class Listy(Record, coerce=True):
    values: List[Inner]


class Nasty(Record, coerce=True):
    choices: Union[Inner, Outer]


@pytest.mark.parametrize(
    "record",
    [
        Inner("solo"),
        Outer(0, Inner("nested")),
        Listy([Inner("one"), Inner("two")]),
        Nasty(Inner("nested")),
        Nasty(Outer(1, Inner("double nested"))),
    ],
)
def test_records(app, record):
    with ctx.context(ctx.app, app):
        codec = Codec(type(record))
        codec.schema_id = 0
        Model = type(record)
        ser = record.dumps(serializer=codec)
        assert_that(ser).is_instance_of(bytes)
        assert_that(Model.loads(ser, serializer=codec)).is_equal_to(record)
