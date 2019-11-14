from datetime import datetime, timezone

from faust.types.tuples import Message

import pytest
from assertpy import assert_that
from faust_avro import AvroSchemaRegistry, CodecException, Record


@pytest.fixture
def asr(request):
    return AvroSchemaRegistry()


@pytest.fixture
def record(request):
    class Person(Record):
        name: str
        age: int
        birth: datetime

    return Person


def test_registry(asr, record):
    # Force the record to have a schema id and the right lookup tables defined for it
    asr.define("unittest", "key", record)
    asr.codecs[record].schema_id = 0

    p = record("Unit Test", 0, datetime(1970, 1, 1, 0, 0, 0, 0, timezone.utc))

    for meth in ["key", "value"]:
        dumps = getattr(asr, f"dumps_{meth}")
        loads = getattr(asr, f"loads_{meth}")

        serialized, headers = dumps(None, p, headers={})
        message = Message("ut-topic", 0, 0, 0, 0, None, serialized, serialized, None)
        record = loads(None, message, headers={})

        assert_that(p).is_equal_to(record)


def test_registry_garbage(asr, record):
    with pytest.raises(CodecException):
        message = Message("ut-topic", 0, 0, 0, 0, None, b"failure", None, None)
        asr.loads_key(None, message, headers={})
