"""GCounter with gossip module."""
import asyncio
from typing import Optional, cast
from functools import reduce
from py_gloomers.node import Node, StdIOTransport, Body, log, reply_to
from py_gloomers.types import BodyFields, MessageTypes


node = Node(transport=StdIOTransport())
INPUT_FIELD = "echo"


class GCounter:
    """A wrapper for the counter operations."""

    counters: dict[str, int]
    event: asyncio.Event
    id: str

    def __init__(self, id: str):
        """Initialize the counter state."""
        self.counters = {id: 0}
        self.event = asyncio.Event()
        self.id = id

    def add(self, delta: int):
        """Add a valueto our local state and broadcast."""
        current = self.counters[self.id]
        self.counters.update({self.id: current + delta})
        # Trigger the broadcast
        self.event.set()

    def state(self) -> int:
        """Return my current value."""
        return self.counters[self.id]

    def merge(self) -> int:
        """Merge states of all the nodes."""
        return reduce(lambda x, y: x + y, self.counters.values(), 0)


g_counter = GCounter(cast(str, node.node_id))


@node.handler
async def read(body: Body) -> Optional[Body]:
    """Handle read message."""
    return {
        BodyFields.TYPE: MessageTypes.READ_OK,
        BodyFields.VALUE: g_counter.merge(),
    } | reply_to(body)


@node.handler
async def add(body: Body) -> Optional[Body]:
    """Handle add message."""
    if (value := body.get(BodyFields.DELTA, None)) is not None:
        # Fire and forget this
        g_counter.add(value)
    return {BodyFields.TYPE: MessageTypes.ADD_OK} | reply_to(body)


@node.handler
async def broadcast(body: Body) -> Optional[Body]:
    """Consume the broadcast RPC."""
    sender = body.get("sender")
    value = body.get(BodyFields.VALUE)
    g_counter.counters.update({sender: value})  # noqa
    return None


@node.worker_func
async def broadcast_worker():
    """Worker for broadcasting data."""
    while True:
        await g_counter.event.wait()
        # Send a broadcast message with Value and Delta received
        body = {
            BodyFields.TYPE: MessageTypes.BROAD,
            BodyFields.VALUE: g_counter.state(),
            "sender": node.node_id,
        }
        for x in node.dns():
            asyncio.create_task(node.rpc(x, body))
        g_counter.event.clear()


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
