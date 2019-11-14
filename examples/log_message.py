"""
Transmit unformatted log messages across kafka and print the resulting messages.
"""

from typing import Dict

from faust_avro import App, Record


class LogMessage(Record):
    fmt: str
    data: Dict[str, str]

    def __str__(self):
        return f"<LogMessage {self.fmt.format(**self.data)}>"


app = App("demo", broker="kafka://localhost")
log_topic = app.topic("logs", value_type=LogMessage)


@app.timer(1.0, on_leader=True)
async def new_msg():
    msg = LogMessage(fmt="A log message with {content}.", data=dict(content="important content"))
    await log_topic.send(value=msg)


@app.agent(log_topic)
async def display(messages):
    async for message in messages:
        print(f"display {message}")
