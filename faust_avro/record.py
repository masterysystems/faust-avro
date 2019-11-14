from typing import ClassVar, Iterable, Optional

import faust


class Record(faust.Record, abstract=True):
    _avro_name: ClassVar[str]
    _avro_aliases: ClassVar[Iterable[str]]

    def __init_subclass__(
        cls,
        avro_name: str = None,
        avro_aliases: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls._avro_name = avro_name or f"{cls.__module__}.{cls.__name__}"
        cls._avro_aliases = avro_aliases or [cls.__name__]

    @classmethod
    def to_avro(cls, registry):
        from faust_avro.parsers.faust import parse

        avro_schema = parse(registry, cls)
        return avro_schema.to_avro()
