import faust

from faust_avro.serializers import AvroSchemaRegistry


class App(faust.App):
    def __init__(self, *args, **kwargs):
        schema = kwargs.setdefault("Schema", AvroSchemaRegistry())
        super().__init__(*args, **kwargs)

        @self.service
        class AvroSchemaRegistryService(faust.Service):
            async def on_start(_):
                """Fetch faust_avro.Record schema ids from the schema registry."""
                await schema.sync()

        @self.command()
        async def register(_):
            """Register faust_avro.Record schemas with the schema registry."""
            await schema.register()

    def topic(self, *args, **kwargs):
        topic = super().topic(*args, **kwargs)

        for which in ["key", "value"]:
            record = kwargs.get(f"{which}_type")
            if record:
                topic.schema.define(topic.topics, which, record)

        return topic
