import json

import pytest
from assertpy import add_extension, assert_that
from faust_avro.asyncio import ConfluentSchemaRegistryClient


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
async def test_compatible(
    client, avro_schema, avro_schema_compatible, subject="test_compatible"
):
    await client.register(subject, avro_schema)
    assert_that(await client.compatible(subject, avro_schema_compatible)).is_true()


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_incompatible(
    client, avro_schema, avro_schema_incompatible, subject="test_incompatible"
):
    await client.register(subject, avro_schema)
    assert_that(await client.compatible(subject, avro_schema_incompatible)).is_false()
