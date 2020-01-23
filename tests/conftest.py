import tempfile
from unittest.mock import patch

import pytest
from faust_avro import App


@pytest.fixture
def app(request):
    with tempfile.TemporaryDirectory() as temp:
        yield App("unittest", datadir=temp)


@pytest.fixture
def asr_sync(app):
    with patch.object(app.avro_schema_registry, "sync", spec=True) as mock:
        yield mock


@pytest.fixture
def asr_schema_by_id(app):
    with patch.object(app.avro_schema_registry, "schema_by_id", spec=True) as mock:
        yield mock


@pytest.fixture
def registry():
    from faust_avro.registry import Registry

    return Registry()


@pytest.fixture
def truck_posting_avsc():
    return dict(
        type="record",
        name="TruckPosting",
        fields=[
            dict(name="origin", type="string", doc="Origin location (city, state)"),
            dict(
                name="dest",
                type=["null", "string"],
                doc="Destination location (city, state)",
            ),
            dict(
                name="type",
                type=dict(
                    type="enum", name="TruckType", symbols=["VAN", "REEFER", "FLATBED"]
                ),
            ),
        ],
    )
