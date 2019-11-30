import asyncio

import click
import faust

from faust_avro.asyncio import ConfluentSchemaRegistryClient
from faust_avro.record import Record
from faust_avro.serializers import Schema
from faust_avro.topic import Topic


class App(faust.App):
    avro_schema_registry: ConfluentSchemaRegistryClient

    def __init__(self, *args, registry_url="http://localhost:8081", **kwargs):
        """Create a new Avro enabled Faust app.

        :param registry_url: The base URL to the schema registry.
        """
        kwargs.setdefault("Schema", Schema)
        kwargs.setdefault("Topic", Topic)
        super().__init__(*args, **kwargs)
        self.avro_schema_registry = ConfluentSchemaRegistryClient(registry_url)

        @self.command()
        async def register(_):
            """Register faust_avro.Record schemas with the schema registry."""
            channels = [agent.channel for agent in _.app.agents.values()]
            topics = [chan for chan in channels if isinstance(chan, Topic)]
            tasks = [topic.compatible(_.app) for topic in topics]
            if all(await asyncio.gather(*tasks)):
                tasks = [topic.register(_.app) for topic in topics]
                await asyncio.gather(*tasks)

        @self.command(faust.cli.argument("model"))
        async def schema(_, model):
            """Dump the schema of a faust_avro.Record model."""
            record = _.import_relative_to_app(model)
            if isinstance(record, type) and issubclass(record, Record):
                _.say(record.to_avro(_.app.avro_schema_registry.registry))
            else:
                raise click.Abort(f"{model} is not an avro-based Record.")
