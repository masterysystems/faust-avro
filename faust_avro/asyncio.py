import contextlib
import typing

import aiohttp

JSON = typing.AsyncGenerator[typing.Dict[str, typing.Any], None]
SchemaID = int
Subject = str
Schema = str


CONTENT_TYPE = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


class ConfluentSchemaRegistryClient:
    """A Confluent AVRO Schema Registry Client.

    Ref: https://docs.confluent.io/1.0/schema-registry/docs/intro.html"""

    def __init__(self, url: str = "http://localhost:8081"):
        """Create a new Confluent schema registry client.

        :param url: The base URL to the schema registry.
        """
        self.url: str = url

    @contextlib.asynccontextmanager
    async def get(self, path: str) -> JSON:
        # We can't use raise_for_status because it causes vcrpy to not write
        # that response into the cassettes.
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url + path) as response:
                yield await response.json()

    @contextlib.asynccontextmanager
    async def post(self, path: str, **json: typing.Any) -> JSON:
        # We can't use raise_for_status because it causes vcrpy to not write
        # that response into the cassettes.
        async with aiohttp.ClientSession(headers=CONTENT_TYPE) as session:
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
            return json["id"]

    async def compatible(self, subject: Subject, schema: Schema) -> bool:
        """
        Check compatibility of a schema with this client's schema registry.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to check compatibility of the schema under.
        :param schema: The schema to check, per the `AVRO specification <https://avro.apache.org/docs/current/spec.html>`_

        :returns: True if the schema is compatible, False otherwise.
        """
        url = f"/compatibility/subjects/{subject}/versions/latest"
        async with self.post(url, schema=schema) as json:
            return json["is_compatible"]

    async def is_registered(self, subject: Subject, schema: Schema) -> bool:
        """
        Check if a schema has been registered with a subject on the client's schema registry.

        :param subject: The `subject <https://docs.confluent.io/current/schema-registry/index.html>`_ to register the schema under.
        :param schema: The schema to register, per the `AVRO specification <https://avro.apache.org/docs/current/spec.html>`_

        """
        async with self.post(f"/subjects/{subject}", schema=schema) as json:
            return "error_code" not in json
