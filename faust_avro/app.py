import click
import faust

from faust_avro.record import Record
from faust_avro.serializers import AvroSchemaRegistry


class App(faust.App):
    def __init__(self, *args, registry_url="http://localhost:8081", **kwargs):
        """Create a new Avro enabled Faust app.

        :param registry_url: The base URL to the schema registry.
        """
        registry = AvroSchemaRegistry(registry_url=registry_url)
        schema = kwargs.setdefault("Schema", registry)
        super().__init__(*args, **kwargs)

        @self.service
        class AvroSchemaRegistryService(faust.Service):
            async def on_start(_):
                """Fetch faust_avro.Record schema ids from the schema registry."""
                await registry.sync()

        @self.command()
        async def register(_):
            """Register faust_avro.Record schemas with the schema registry."""
            await registry.register()

        @self.command(faust.cli.argument('model'))
        async def schema(app, model):
            """Dump the schema of a faust_avro.Record model."""
            record = app.import_relative_to_app(model)
            if isinstance(record, type) and issubclass(record, Record):
                app.say(await registry.to_avro(record))
            else:
                raise click.Abort(f"{model} is not an avro-based Record.")

    def topic(self, *args, **kwargs):
        topic = super().topic(*args, **kwargs)

        for which in ["key", "value"]:
            record = kwargs.get(f"{which}_type")
            if record:
                topic.schema.define(topic.topics, which, record)

        return topic
