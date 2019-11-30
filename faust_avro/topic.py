from typing import Any, Tuple

import faust
from faust.types import AppT, CodecArg, SchemaT
from faust.types.core import K, OpenHeadersArg, V

from faust_avro.context import context, topic
from faust_avro.serializers import Schema


class Topic(faust.Topic):
    """A modified faust.Topic that injects itself into the schema.dumps call."""

    schema: Schema

    def prepare_key(
        self,
        key: K,
        key_serializer: CodecArg,
        schema: SchemaT = None,
        headers: OpenHeadersArg = None,
    ) -> Tuple[Any, OpenHeadersArg]:
        """Serialize key to format suitable for transport."""
        with context(topic, self):
            return super().prepare_key(key, key_serializer, schema, headers)

    def prepare_value(
        self,
        value: V,
        value_serializer: CodecArg,
        schema: SchemaT = None,
        headers: OpenHeadersArg = None,
    ) -> Tuple[Any, OpenHeadersArg]:
        """Serialize value to format suitable for transport."""
        with context(topic, self):
            return super().prepare_value(value, value_serializer, schema, headers)

    async def compatible(self, app: AppT) -> bool:
        return all(await self.schema.compatible(app, self))

    async def register(self, app: AppT) -> None:
        await self.schema.register(app, self)

    async def sync(self, app: AppT) -> None:
        await self.schema.sync(app, self)
