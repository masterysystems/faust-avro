import asyncio
import json
import struct
from dataclasses import dataclass, field, InitVar
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Type, cast

import fastavro
import faust
import funcy
from cached_property import cached_property
from faust.types.app import AppT
from faust.types.codecs import CodecArg
from faust.types.core import K, OpenHeadersArg, V
from faust.types.serializers import KT, VT
from faust.types.tuples import Message

from faust_avro.asyncio import ConfluentSchemaRegistryClient as CSRC, run_in_thread
from faust_avro.exceptions import CodecException
from faust_avro.record import Record
from faust_avro.registry import Registry

SchemaID = int
SubjectT = str


HEADER = struct.Struct(">bI")
MAGIC_BYTE = 0


@dataclass
class AvroCodec:
    record: Record
    registry: Registry
    subjects: List[SubjectT] = field(default_factory=list)
    schema_id: Optional[SchemaID] = None
    schema: InitVar[Any] = None

    def __post_init__(self, schema=None):
        self.schema = fastavro.parse_schema(self.record.to_avro(self.registry))

    async def compatible(self, registry: CSRC):
        schema = json.dumps(self.record.to_avro(self.registry))
        tasks = [registry.compatible(subject, schema) for subject in self.subjects]
        compatible = await asyncio.gather(*tasks)
        failed = [subj for subj, valid in zip(self.subjects, compatible) if not valid]
        if failed:
            print(f"{self.record.__module__}.{self.record.__name__} incompatible for: {failed}")

        return all(compatible)

    async def register(self, registry: CSRC):
        schema = json.dumps(self.record.to_avro(self.registry))
        tasks = [registry.register(subject, schema) for subject in self.subjects]
        self.schema_id, *ids = funcy.distinct(await asyncio.gather(*tasks))
        assert not ids, "Schemas must share the same id across all subjects!"
        print(f"{self.record.__module__}.{self.record.__name__} registered as schema id {self.schema_id} for: {self.subjects}")

    async def sync(self, registry: CSRC):
        schema = json.dumps(self.record.to_avro(self.registry))
        self.schema_id = await registry.sync(self.subjects[0], schema)

    def dumps(self, value: Record) -> bytes:
        payload = BytesIO()
        fastavro.schemaless_writer(payload, self.schema, value.to_representation())
        return payload.getvalue()

    def loads(self, s: bytes) -> Record:
        return self.record(**fastavro.schemaless_reader(BytesIO(s), self.schema))


class AvroCodecDict(dict):
    # TODO: Replace with functools.cached_property in python 3.8
    @cached_property
    def _registry(self):
        # This handles creating a single Registry instance per AvroSchemaRegistry
        # instance which also means a single Registry instance per App instance.
        return Registry()

    def __missing__(self, record: Record):
        return self.setdefault(record, AvroCodec(record, self._registry))


class AvroSchemaRegistry(faust.Schema):
    def __init__(self, *, registry_url="http://localhost:8081", **kwargs):
        super().__init__(**kwargs)
        self.codecs: Dict[Type[Record], AvroCodec] = AvroCodecDict()
        self.registry: CSRC = CSRC(registry_url)

    def __call__(self, *args, **kwargs):
        # Normally a class gets passed as the App's Schema parameter.
        # That doesn't work for avro, so instead of implementing the
        # borg pattern, we just define __call__ to return ourselves and
        # ignore any passed arguments.
        #
        # If the passed arguments included the topic in addition to the
        # key and value, then we could do self.define's work here and
        # wouldn't need to wrap the App's topic method at all.
        return self

    def __str__(self):  # pragma: no cover
        return f"<AvroSchemaRegistry registry={self.registry}>"

    def define(self, topics: List[str], subject_type: str, record: Record):
        subjects = [f"{topic}-{subject_type}" for topic in topics]
        self.codecs[record].subjects.extend(subjects)

    @funcy.memoize
    def get_codec_by_id(self, schema_id: SchemaID) -> AvroCodec:
        for codec in self.codecs.values():
            # We should do this outside the loop and in parallel. In theory the startup agent should do it but in practice it looks like we receive messages
            # before that agent has finished running, meaning that we are in an unready state.
            if codec.schema_id is None:
                # This is a hack to get around the dropped async context
                run_in_thread(codec.register(self.registry))
            if codec.schema_id == schema_id:
                return codec
        else:
            raise NotImplementedError("Should fetch from schema registry via schema_id")

    async def sync(self):
        tasks = [codec.sync(self.registry) for codec in self.codecs.values()]
        await asyncio.gather(*tasks)

    async def register(self):
        compatible = [codec.compatible(self.registry) for codec in self.codecs.values()]
        if all(await asyncio.gather(*compatible)):
            tasks = [codec.register(self.registry) for codec in self.codecs.values()]
            await asyncio.gather(*tasks)
        else:
            raise Exception("Incompatible Schemas")

    def _loads(self, payload: bytes) -> Record:
        """Decompose a confluent client message into the schema id and payload bytes"""

        header, payload = payload[:5], payload[5:]
        magic, schema_id = HEADER.unpack(header)

        if magic != MAGIC_BYTE:
            raise CodecException(f"Bad magic byte: {magic}.")

        return self.get_codec_by_id(schema_id).loads(payload)

    def _dumps(self, data: Record) -> bytes:
        """Dump the schema id and payload into a confluent client message."""

        codec = self.codecs[type(data)]
        if codec.schema_id is None:
            # This is a hack to get around the dropped async context
            run_in_thread(codec.register(self.registry))
        header = HEADER.pack(MAGIC_BYTE, codec.schema_id)
        return header + codec.dumps(data)

    def loads_key(self, app: AppT, message: Message, **kwargs) -> KT:
        # TODO -- should we raise error if kwargs is not empty?
        return cast(KT, None if message.key is None else self._loads(message.key))

    def loads_value(self, app: AppT, message: Message, **kwargs) -> VT:
        # TODO -- should we raise error if kwargs is not empty?
        return cast(VT, None if message.value is None else self._loads(message.value))

    def dumps_key(
        self, app: AppT, key: K, *, serializer: CodecArg = None, headers: OpenHeadersArg
    ) -> Tuple[Any, OpenHeadersArg]:
        return self._dumps(cast(Record, key)), headers

    def dumps_value(
        self,
        app: AppT,
        value: V,
        *,
        serializer: CodecArg = None,
        headers: OpenHeadersArg,
    ) -> Tuple[Any, OpenHeadersArg]:
        return self._dumps(cast(Record, value)), headers
