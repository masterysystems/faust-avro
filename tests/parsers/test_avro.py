import pytest
from assertpy import assert_that
from faust_avro import SchemaAlreadyDefinedError, UnknownTypeError


def avsc(type="null", **kwargs):
    return dict(type=type, **kwargs)


def record(name, **kwargs):
    return avsc("record", name=name, **kwargs)


def field(name, type="null", **kwargs):
    return avsc(type, name=name, **kwargs)


def enum(name, *symbols, **kwargs):
    return avsc("enum", name=name, symbols=list(symbols), **kwargs)


def array(items="null"):
    return avsc(type="array", items=items)


def mapping(values="null"):
    return avsc(type="map", values=values)


def fixed(name, size=16, **kwargs):
    return avsc(type="fixed", name=name, size=size, **kwargs)


@pytest.mark.parametrize(
    "avro",
    [
        # Primitive types
        "null",
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "bytes",
        "string",
        # Complex types
        record("Record", fields=[field("field", "boolean", default=True)]),
        enum("enumeration", "red", "green", "blue"),
        array("string"),
        mapping("boolean"),
        fixed("uuidish"),
        ["null", "int"],  # a union
        # Logical types
        # Decimal isn't supported yet...
        # avsc(type="bytes", logicalType="decimal", precision=2),
        # avsc(type="bytes", logicalType="decimal", precision=4, scale=2),
        avsc(type="string", logicalType="uuid"),
        avsc(type="int", logicalType="date"),
        avsc(type="int", logicalType="time-millis"),
        avsc(type="long", logicalType="time-micros"),
        avsc(type="long", logicalType="timestamp-millis"),
        avsc(type="long", logicalType="timestamp-micros"),
        fixed("duration", size=12, logicalType="duration"),
        # Must ignore (but preserve) unknown logical types
        avsc(type="string", logicalType="unknown"),
        # Edge cases
        avsc(),  # silly nested {"type"="null"}
        avsc(avsc()),  # double nested
        record("Recursive", fields=[field("inner", ["null", "Recursive"])]),
    ],
)
def test_avro(registry, avro):
    assert_that(registry.parse(avro).to_avro()).is_equal_to(avro)
    if "name" in avro:
        assert_that(registry).contains(avro["name"])


@pytest.mark.parametrize(
    "exception,avro",
    [
        (
            SchemaAlreadyDefinedError,
            [
                dict(type="enum", name="Dupe", symbols=["DU", "PLI", "CATE"]),
                dict(type="enum", name="Dupe", symbols=["Dept", "of", "Redunendcy"]),
            ],
        ),
        (UnknownTypeError, b"str"),  # bytearray, rather than string fails
        (UnknownTypeError, dict(type="rabbit_of_caerbannog")),
    ],
)
def test_avro_garbage(registry, exception, avro):
    with pytest.raises(exception):
        registry.parse(avro)
