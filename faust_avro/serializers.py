import asyncio
import collections.abc
import contextlib
import functools
import json
import struct
from io import BytesIO
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Tuple, Type

import fastavro
import faust
import funcy
from faust.serializers import codecs
from faust.types import TopicT
from faust.types.app import AppT
from faust.types.codecs import CodecArg
from faust.types.core import K, OpenHeadersArg, V
from faust.types.models import ModelArg
from faust.types.serializers import KT, VT
from faust.types.tuples import Message

import faust_avro.context as ctx
from faust_avro.asyncio import run_in_thread
from faust_avro.record import Record

SchemaID = int
SubjectT = str


HEADER = struct.Struct(">bI")
MAGIC_BYTE = 0


@functools.singledispatch
def faust_annotate(value) -> Any:
    # Ensure that the whole value tree structure has been pushed through
    # to_representation() so that we aren't passing Record subclass instances
    # to fastavro.
    try:
        return faust_annotate(value.to_representation())
    except AttributeError:
        return value


@faust_annotate.register
def faust_annotate_dict(value: collections.abc.Mapping) -> collections.abc.Mapping:
    return dict([(k, faust_annotate(v)) for k, v in value.items()])


@faust_annotate.register(set)
@faust_annotate.register
def faust_annotate_list(value: list) -> list:
    return list([faust_annotate(item) for item in value])


class Codec(codecs.Codec):
    def __init__(self, record: Type[Record], **kwargs: Any):
        super().__init__(**kwargs)
        self.record: Type[Record] = record
        self.name = f"{self.record.__module__}.{self.record.__name__}"
        self.schema_id: Optional[int] = None
        self.versions: Dict[int, str] = dict()

    @funcy.memoize
    def dict_schema(self, app: AppT) -> Dict[str, Any]:
        return self.record.to_avro(app.avro_schema_registry.registry)

    @funcy.memoize
    def schema(self, app: AppT) -> str:
        return json.dumps(self.dict_schema(app))

    def _dumps(self, value: V) -> bytes:
        app = ctx.app.get()

        # TODO: get async passed down the faust call stack so that this can
        # be an await in this loop, rather than using threading to spawn a
        # new loop and block the main loop on it anyway.
        if self.schema_id is None:
            run_in_thread(self.sync(app, ctx.subject.get()))

        header = HEADER.pack(MAGIC_BYTE, self.schema_id)
        payload = BytesIO()
        schema = self.dict_schema(app)

        fastavro.schemaless_writer(payload, schema, faust_annotate(value))
        return header + payload.getvalue()

    def _loads(self, payload: bytes) -> Any:
        app = ctx.app.get()
        header, payload = payload[:5], payload[5:]
        magic, schema_id = HEADER.unpack(header)

        if magic != MAGIC_BYTE:
            raise faust.exceptions.ValueDecodeError(f"Bad magic byte: {magic}.")

        # TODO: get async passed down the faust call stack so that this can
        # be an await in this loop, rather than using threading to spawn a
        # new loop and block the main loop on it anyway.
        if self.schema_id is None:
            run_in_thread(self.sync(app, ctx.subject.get()))

        if schema_id != self.schema_id:
            # TODO: get async passed down the faust call stack so that this can
            # be an await in this loop, rather than using threading to spawn a
            # new loop and block the main loop on it anyway.
            if schema_id not in self.versions:
                run_in_thread(self.schema_by_id(app, schema_id))

            return fastavro.schemaless_reader(
                BytesIO(payload),
                self.versions[schema_id],
                self.dict_schema(app),
                return_record_name=True,
            )

        return fastavro.schemaless_reader(
            BytesIO(payload), self.dict_schema(app), return_record_name=True
        )

    async def schema_by_id(self, app: AppT, schema_id: SchemaID) -> None:
        self.versions[schema_id] = json.loads(
            await app.avro_schema_registry.schema_by_id(schema_id)
        )

    async def compatible(self, app: AppT, subject: SubjectT) -> bool:
        ok = await app.avro_schema_registry.compatible(subject, self.schema(app))
        if not ok:
            print(f"{self.name} is compatible with {subject}.")
        return ok

    async def register(self, app: AppT, subject: SubjectT) -> None:
        schema_id = await app.avro_schema_registry.register(subject, self.schema(app))
        print(f"{self.name} registered as schema id {schema_id} on {subject}")

    async def sync(self, app: AppT, subject: SubjectT) -> None:
        self.schema_id = await app.avro_schema_registry.sync(subject, self.schema(app))


class Schema(faust.Schema):
    """An avro compatible faust Schema."""

    def update(
        self,
        *,
        key_type: ModelArg = None,
        value_type: ModelArg = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        allow_empty: bool = None,
    ) -> None:
        if key_type is not None and issubclass(key_type, Record):
            key_serializer = Codec(key_type)
        if value_type is not None and issubclass(value_type, Record):
            value_serializer = Codec(value_type)
        super().update(
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            allow_empty=allow_empty,
        )

    def _spray(self, app: AppT, topic: TopicT, method) -> Iterator[Awaitable[Any]]:
        for topic_name in topic.topics:
            if self.key_serializer is not None:
                yield method(self.key_serializer, app, f"{topic_name}-key")
            if self.value_serializer is not None:
                yield method(self.value_serializer, app, f"{topic_name}-value")

    async def compatible(self, app: AppT, topic: TopicT) -> List[bool]:
        return await asyncio.gather(*list(self._spray(app, topic, Codec.compatible)))

    async def register(self, app: AppT, topic: TopicT) -> None:
        return await asyncio.gather(*list(self._spray(app, topic, Codec.register)))

    async def sync(self, app: AppT, topic: TopicT) -> None:
        return await asyncio.gather(*list(self._spray(app, topic, Codec.sync)))

    @contextlib.contextmanager
    def context(self, app: AppT, subject: SubjectT):
        with ctx.context(ctx.app, app), ctx.context(ctx.subject, subject):
            yield

    def loads_key(
        self,
        app: AppT,
        message: Message,
        *,
        loads: Callable = None,
        serializer: CodecArg = None,
    ) -> KT:
        with self.context(app, f"{message.topic}-key"):
            return super().loads_key(app, message, loads=loads, serializer=serializer)

    def loads_value(
        self,
        app: AppT,
        message: Message,
        *,
        loads: Callable = None,
        serializer: CodecArg = None,
    ) -> VT:
        with self.context(app, f"{message.topic}-value"):
            return super().loads_value(app, message, loads=loads, serializer=serializer)

    def dumps_key(
        self,
        app: AppT,
        key: K,
        *,
        serializer: CodecArg = None,
        headers: OpenHeadersArg,
    ) -> Tuple[Any, OpenHeadersArg]:
        topic_name, *_ = ctx.topic.get().topics
        with self.context(app, f"{topic_name}-key"):
            return super().dumps_key(app, key, serializer=serializer, headers=headers)

    def dumps_value(
        self,
        app: AppT,
        value: V,
        *,
        serializer: CodecArg = None,
        headers: OpenHeadersArg,
    ) -> Tuple[Any, OpenHeadersArg]:
        topic_name, *_ = ctx.topic.get().topics
        with self.context(app, f"{topic_name}-value"):
            return super().dumps_value(
                app, value, serializer=serializer, headers=headers
            )
