"""Workload for broadcast."""
import asyncio
from typing import Optional, Coroutine
from py_gloomers.node import Node, StdIOTransport, Body, log, Service
from py_gloomers.types import BodyFields, MessageTypes, MessageError, \
    ErrorType
from py_gloomers.node import reply_to


node = Node(transport=StdIOTransport())


class GCounter:
    """The GCounter workload."""

    service: Service
    counter: int

    def __init__(self):
        """Initialize the workload."""
        self.service = Service("lin-kv", node)
        self.counter = 0

    async def add(self, number: int):
        """Increase the counter."""
        pass

    async def swap(self, number: int):
        """Swap the counter for a different value."""
        pass


g_counter = GCounter()


@node.handler
async def read(body: Body) -> Optional[Body]:
    """Handle read message."""
    # 1. Always return the status of the internal counter
    pass


@node.handler
async def add(body: Body) -> Optional[Body]:
    """Handle add message."""
    pass


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
