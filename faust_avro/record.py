from typing import Any, Callable, ClassVar, Dict, Iterable, Optional

import faust
from faust.models.record import _maybe_to_representation  # noqa: F401
from faust.types.models import FieldDescriptorT, ModelT
from faust.utils import codegen


# WORKAROUND for upstream bug
def _maybe_has_to_representation(val: ModelT = None) -> Optional[Any]:
    return (
        val.to_representation()
        if val is not None and hasattr(val, "to_representation")
        else val
    )


class Record(faust.Record, abstract=True):
    _avro_name: ClassVar[str]
    _avro_aliases: ClassVar[Iterable[str]]

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("include_metadata", False)
        super().__init__(*args, **kwargs)

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
    def to_avro(cls, registry) -> Dict[str, Any]:
        from faust_avro.parsers.faust import parse

        avro_schema = parse(registry, cls)
        return avro_schema.to_avro()

    # WORKAROUND for upstream bug
    @classmethod
    def _BUILD_asdict(cls) -> Callable[..., Dict[str, Any]]:
        preamble = [
            "return self._prepare_dict({",
        ]

        fields = [
            f"  {d.output_name!r}: {cls._BUILD_asdict_field(name, d)},"
            for name, d in cls._options.descriptors.items()
            if not d.exclude
        ]

        postamble = [
            "})",
        ]

        return codegen.Method(
            "_asdict",
            [],
            preamble + fields + postamble,
            globals=globals(),
            locals=locals(),
        )

    # WORKAROUND for upstream bug
    @classmethod
    def _BUILD_asdict_field(cls, name: str, field: FieldDescriptorT) -> str:
        return f"_maybe_has_to_representation(self.{name})"
