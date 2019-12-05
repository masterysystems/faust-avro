"""
Conext Vars are used here to avoid having to overload many methods of the send/receive
message traces in order to pass the topic/app/subject down from higher levels (eg, topic.send, schema.loads_*)
to lower levels (eg, Codec.dumps/loads) in order to be able to perform schema lookups
with the schema registry without having to pass topic/app/subject args through the
full call stack between them.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from faust.types import TopicT
from faust.types.app import AppT

app: ContextVar[AppT] = ContextVar("app")
subject: ContextVar[str] = ContextVar("subject")
topic: ContextVar[TopicT] = ContextVar("topic")


@contextmanager
def context(var: ContextVar, value: Any):
    token = var.set(value)
    yield
    var.reset(token)
