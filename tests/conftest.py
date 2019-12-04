import tempfile

import pytest
from faust_avro import App


@pytest.fixture
def app(request):
    with tempfile.TemporaryDirectory() as temp:
        yield App("unittest", datadir=temp)


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
