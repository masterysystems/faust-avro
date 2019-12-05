import json
import threading

import pytest
from assertpy import add_extension, assert_that
from faust_avro.asyncio import (
    ConfluentSchemaRegistryClient,
    SchemaNotFound,
    SubjectNotFound,
    run_in_thread,
)


@pytest.fixture
def client():
    return ConfluentSchemaRegistryClient()


@pytest.fixture
def avro_schema():
    return json.dumps(
        dict(type="record", name="UnitTest", fields=[dict(name="field", type="string")])
    )


@pytest.fixture
def avro_schema_compatible():
    return json.dumps(
        dict(
            type="record",
            name="UnitTest",
            fields=[
                dict(name="field", type="string"),
                dict(name="second", type="string", default=""),
            ],
        )
    )


@pytest.fixture
def avro_schema_incompatible():
    return json.dumps(
        dict(
            type="record",
            name="UnitTest",
            fields=[
                dict(name="field", type="string"),
                dict(name="second", type="string"),
            ],
        )
    )


def is_same_schema(self, schema):
    assert_that(json.loads(self.val)).is_equal_to(json.loads(schema))


@pytest.fixture(scope="module", autouse=True)
def my_extensions():
    add_extension(is_same_schema)


def test_run_in_thread():
    event = threading.Event()

    async def set_event():
        event.set()

    run_in_thread(set_event())
    assert_that(event).has_is_set(True)


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_register(client, avro_schema, subject="test_register"):
    result = await client.register(subject, avro_schema)
    assert_that(result).is_type_of(int)
    schema_id = await client.sync(subject, avro_schema)
    assert_that(schema_id).is_type_of(int).is_equal_to(result)
    assert_that(await client.subjects()).contains(subject)
    assert_that(await client.schema_by_id(result)).is_same_schema(avro_schema)
    assert_that(await client.schema_by_topic(subject)).is_same_schema(avro_schema)


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_registered(client, avro_schema, subject="test_registered"):
    await client.register(subject, avro_schema)
    assert_that(await client.is_registered(subject, avro_schema)).is_true()


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_unregistered(client, avro_schema, subject="test_unregistered"):
    assert_that(await client.is_registered(subject, avro_schema)).is_false()


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_sync_no_subject(client, avro_schema, subject="test_sync_no_subject"):
    with pytest.raises(SubjectNotFound):
        await client.sync(subject, avro_schema)


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_sync_no_schema(
    client, avro_schema, avro_schema_compatible, subject="test_sync_no_schema"
):
    await client.register(subject, avro_schema)
    with pytest.raises(SchemaNotFound):
        await client.sync(subject, avro_schema_compatible)


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_compatible(client, avro_schema, subject="test_compatible"):
    assert_that(await client.compatible(subject, avro_schema)).is_true()


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_self_compatible(client, avro_schema, subject="test_self_compatible"):
    await client.register(subject, avro_schema)
    assert_that(await client.compatible(subject, avro_schema)).is_true()


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_incompatible(
    client, avro_schema, avro_schema_incompatible, subject="test_incompatible"
):
    await client.register(subject, avro_schema)
    assert_that(await client.compatible(subject, avro_schema_incompatible)).is_false()
