from datetime import datetime, timezone

from faust.exceptions import ValueDecodeError
from faust.types.tuples import Message

import pytest
from assertpy import assert_that
from faust_avro import Record
from faust_avro import context as ctx
from faust_avro.serializers import Codec


class Key(Record):
    idx: int


class Person(Record):
    name: str
    age: int
    birth: datetime


@pytest.fixture
def topic(app):
    t = app.topic("people", key_type=Key, value_type=Person)
    with ctx.context(ctx.topic, t):
        t.schema.key_serializer.schema_id = 0
        t.schema.value_serializer.schema_id = 1
        yield t
        t.schema.key_serializer.schema_id = None
        t.schema.value_serializer.schema_id = None


def test_codec_not_implemented():
    with pytest.raises(NotImplementedError):
        Codec(Person, kwargs=True)

    c = Codec(Person)
    with pytest.raises(NotImplementedError):
        c.clone()
    with pytest.raises(NotImplementedError):
        c | True


def test_key_serde(app, topic):
    k = Key(1)

    payload, headers = topic.prepare_key(k, None)
    message = Message("ut-topic", 0, 0, 0, 0, None, payload, None, None)
    record = topic.schema.loads_key(app, message)

    assert_that(k).is_equal_to(record)


def test_value_serde(app, topic):
    v = Person("Unit Test", 0, datetime(1970, 1, 1, 0, 0, 0, 0, timezone.utc))

    payload, headers = topic.prepare_value(v, None)
    message = Message("ut-topic", 0, 0, 0, 0, None, None, payload, None)
    record = topic.schema.loads_value(app, message)

    assert_that(v).is_equal_to(record)


def test_garbage(app, topic):
    message = Message("ut-topic", 0, 0, 0, 0, None, None, b"failure", None)
    with pytest.raises(ValueDecodeError):
        topic.schema.loads_value(app, message)
