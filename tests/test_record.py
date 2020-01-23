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
    untranslated: Union[int, float]


@pytest.mark.parametrize(
    "record",
    [
        Inner("solo"),
        Outer(0, Inner("nested")),
        Listy([Inner("one"), Inner("two")]),
        Nasty(Inner("nested"), 1),
        Nasty(Outer(1, Inner("double nested")), 0.5),
    ],
)
def test_records(app, asr_sync, asr_schema_by_id, record):
    with ctx.context(ctx.app, app), ctx.context(ctx.subject, "unittest"):
        codec = Codec(type(record))
        asr_schema_by_id.return_value = codec.schema(app)

        asr_sync.return_value = 0
        codec.schema_id = None
        Model = type(record)
        ser = record.dumps(serializer=codec)
        assert_that(ser).is_instance_of(bytes)

        asr_sync.return_value = 1
        codec.schema_id = None
        deser = Model.loads(ser, serializer=codec)
        assert_that(deser).is_equal_to(record)
