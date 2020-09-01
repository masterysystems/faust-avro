import asyncio
import contextlib
import threading
import typing

import aiohttp

from faust_avro.registry import Registry

JSON = typing.AsyncGenerator[typing.Dict[str, typing.Any], None]
SchemaID = int
Subject = str
Schema = str


GET = {"Accept": "application/vnd.schemaregistry.v1+json"}
POST = {
    "Content-Type": "application/vnd.schemaregistry.v1+json",
    "Accept": "application/vnd.schemaregistry.v1+json",
}


def _run_in_thread(coro: typing.Awaitable) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)


def run_in_thread(coro: typing.Awaitable) -> None:
    """Runs a coroutine in a separate loop/thread.

    Use this when you're running in a loop, but have had the `async def`
    dropped off your backtrace, so that you can't just 'await' directly.

    Mostly, just don't use this.
    """
    thread = threading.Thread(target=_run_in_thread, args=(coro,))
    thread.start()
    thread.join()


class SchemaException(Exception):
    """A generic async schema registry client exception."""


class SchemaNotFound(SchemaException):
    """The schema has not been registered with the schema registry for that subject."""


class SubjectNotFound(SchemaException):
    """The schema registry has no such subject."""


class ConfluentSchemaRegistryClient:
    """A Confluent AVRO Schema Registry Client.

    Ref: https://docs.confluent.io/1.0/schema-registry/docs/intro.html"""

    def __init__(self, url: str = "http://localhost:8081"):
        """Create a new Confluent schema registry client.

        :param url: The base URL to the schema registry.
        """
        self.url: str = url
        self.registry: Registry = Registry()

    @contextlib.asynccontextmanager
    async def get(self, path: str) -> JSON:
        # We can't use raise_for_status because it causes vcrpy to not write
        # that response into the cassettes.
        async with aiohttp.ClientSession(headers=GET) as session:
            async with session.get(self.url + path) as response:
                yield await response.json()

    @contextlib.asynccontextmanager
    async def post(self, path: str, **json: typing.Any) -> JSON:
        # We can't use raise_for_status because it causes vcrpy to not write
        # that response into the cassettes.
        async with aiohttp.ClientSession(headers=POST) as session:
            async with session.post(self.url + path, json=json) as response:
                yield await response.json()

    async def subjects(self) -> typing.List[Subject]:
        """
        Fetch a list of all known subjects in the schema registry.

        :returns: A list of `subjects <https://docs.confluent.io/current/schema-registry/index.html>`_.
        """
        async with self.get("/subjects") as json:
            return typing.cast(typing.List[Subject], json)

    async def schema_by_topic(self, subject: Subject) -> Schema:
        """
        Fetch the current schema from the client's schema registry by subject.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to register the schema under.

        :returns: The schema's definition.
        """
        async with self.get(f"/subjects/{subject}/versions/latest") as json:
            return json["schema"]

    async def schema_by_id(self, id: SchemaID) -> Schema:
        """
        Fetch a schema from the client's schema registry by schema id.

        :param id: The id of the schema.

        :returns: The schema's definition.
        """
        async with self.get(f"/schemas/ids/{id}") as json:
            return json["schema"]

    async def register(self, subject: Subject, schema: Schema) -> SchemaID:
        """
        Register a schema with this client's schema registry.

        If the schema was already registered under the subject, nothing happens.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to register the schema under.
        :param schema: The schema to register, per the `AVRO specification <https://avro.apache.org/docs/current/spec.html>`_

        :returns: The id of the schema.
        """
        async with self.post(f"/subjects/{subject}/versions", schema=schema) as json:
            return json["id"]

    async def sync(self, subject: Subject, schema: Schema) -> SchemaID:
        """
        Fetch the schema id for a previously registered schema with this client's schema registry.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to register the schema under.
        :param schema: The schema to register, per the `AVRO specification <https://avro.apache.org/docs/current/spec.html>`_

        :returns: The id of the schema.
        """
        async with self.post(f"/subjects/{subject}", schema=schema) as json:
            if json.get("error_code", False) == 40401:
                # "Subject not found" -- no schemas ever registered on this topic-key/value.
                raise SubjectNotFound(subject)
            if json.get("error_code", False) == 40403:
                # "Schema not found" -- this schema has not been registered.
                raise SchemaNotFound(schema)
            try:
                return json["id"]
            except KeyError:
                raise SchemaException(json)

    async def compatible(self, subject: Subject, schema: Schema) -> bool:
        """
        Check compatibility of a schema with this client's schema registry.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to check compatibility of the schema under.
        :param schema: The schema to check, per the `AVRO specification <https://avro.apache.org/docs/current/spec.html>`_

        :returns: True if the schema is compatible, False otherwise.
        """
        url = f"/compatibility/subjects/{subject}/versions/latest"
        async with self.post(url, schema=schema) as json:
            # https://docs.confluent.io/1.0/schema-registry/docs/api.html#post--compatibility-subjects-(string-%20subject)-versions-(versionId-%20version)
            if json.get("error_code", False) in [404, 40401]:
                # "Subject not found" so as the first upload, it will be compatible with itself.
                return True
            return json["is_compatible"]

    async def is_registered(self, subject: Subject, schema: Schema) -> bool:
        """
        Check if a schema has been registered with a subject on the client's schema registry.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to register the schema under.
        :param schema: The schema to register, per the `AVRO specification <https://avro.apache.org/docs/current/spec.html>`_

        """
        async with self.post(f"/subjects/{subject}", schema=schema) as json:
            return "error_code" not in json
