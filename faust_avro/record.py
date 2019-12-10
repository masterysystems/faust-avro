import types
from functools import lru_cache
from typing import Any, Callable, ClassVar, Dict, Iterable, Optional, Type

import faust
from faust.models.fields import TYPE_TO_FIELD, FieldDescriptor
from faust.models.record import _maybe_to_representation  # noqa: F401
from faust.types.models import FieldDescriptorT, ModelT
from faust.utils import codegen
from typing_inspect import is_union_type


# WORKAROUND for upstream bug
# https://github.com/robinhood/faust/issues/386
def _maybe_has_to_representation(val: ModelT = None) -> Optional[Any]:
    return (
        val.to_representation()
        if val is not None and hasattr(val, "to_representation")
        else val
    )


class UnionField(FieldDescriptor):
    def prepare_value(self, value: Any, *, coerce: bool = None) -> Optional[str]:
        if self.should_coerce(value, coerce):
            for klass in self.type.__args__:
                try:
                    return klass.from_data(value)
                except TypeError:
                    # Wrong values passed, it wasn't this branch of the union
                    pass
                except AttributeError:
                    # Likely a primitive, the return value below will take
                    # care of selecting the right primitive
                    pass

        return value


# MONKEY PATCH faust's field_for_type until we can get this upstreamed
@lru_cache(maxsize=2048)
def field_for_type(typ: Type) -> Type[FieldDescriptorT]:
    try:
        return TYPE_TO_FIELD[typ]
    except KeyError:
        for item, DescriptorType in TYPE_TO_FIELD.items():
            if isinstance(item, types.FunctionType) and item(typ):
                return DescriptorType

            try:
                if issubclass(typ, item):
                    return DescriptorType
            except TypeError:
                continue
        return FieldDescriptor


faust.models.record.field_for_type = field_for_type


TYPE_TO_FIELD[is_union_type] = UnionField


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
    # https://github.com/robinhood/faust/issues/386
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
    # https://github.com/robinhood/faust/issues/386
    @classmethod
    def _BUILD_asdict_field(cls, name: str, field: FieldDescriptorT) -> str:
        modelattrs = cls._options.modelattrs
        is_model = name in modelattrs
        if is_model:
            generic = modelattrs[name]
            if generic is list or generic is tuple:
                return (
                    f"[v.to_representation() for v in self.{name}] "
                    f"if self.{name} is not None else None"
                )
            elif generic is set:
                return f"self.{name}"
            elif generic is dict:
                return (
                    f"{{k: v.to_representation() "
                    f"  for k, v in self.{name}.items()}}"
                )
            else:
                return f"_maybe_to_representation(self.{name})"
        else:
            return f"_maybe_has_to_representation(self.{name})"
