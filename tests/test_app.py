import subprocess
import tempfile

import pytest
from assertpy import assert_that
from faust_avro import App, AvroSchemaRegistry, Record


@pytest.mark.vcr()
@pytest.mark.asyncio
async def test_app():
    """
    This test uses vcrpy to record the interactions with the schema registry.
    If those change, the test will fail. The easy way to fix them is to:
        docker-compose up -d
        rm -rf tests/cassettes
        pytest
    And commit the resulting changes to the cassettes to the git repo. If tests
    still fail, cycle the last 2 commands (rm;pytest) while debugging.
    """

    # This prevents leaving garbage around the filesystem.
    with tempfile.TemporaryDirectory() as temp:

        class Person(Record):
            name: str
            age: int

        app = App("unittest", datadir=temp)
        people = app.topic("people", value_type=Person)

        # faust_avro apps must have AvroSchemaRegistry as the schema of a topic
        assert_that(people.schema).is_type_of(AvroSchemaRegistry)


def test_schema():
    # This test is really slow -- does it add enough value to keep it?
    # No negative test because I don't want it slower...
    cmd = """python -m faust -A examples.log_message schema examples.log_message.LogMessage"""
    result = subprocess.run(cmd.split(), check=True, stdout=subprocess.PIPE)
    assert_that(result.stdout).is_equal_to(
        b'{"type": "record", "name": "examples.log_message.LogMessage", "aliases": ["LogMessage"], "fields": [{"type": "string", "name": "fmt"}, {"type": {"type": "map", "values": "string"}, "name": "data"}]}\n'
    )
