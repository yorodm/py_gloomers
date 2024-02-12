"""G-Set workload."""
import asyncio
from typing import Optional
from py_gloomers.node import StdIOTransport, Node, Body, reply_to
from py_gloomers.types import MessageTypes, BodyFields, MessageError, ErrorType


node = Node(StdIOTransport())


class GSet:
    """Simple CRDT."""

    state: set

    def __init__(self):
        """Create the GSet CRDT."""
        self.state = set()

    def current_state(self):
        """Return the current state."""
        return list(self.state)

    def add(self, value: int):
        """Add an element to the state."""
        self.state.add(value)

    def update(self, values: list):
        """Update the state."""
        self.state.update(values)


gset = GSet()


VALUE_FIELD = "value"
ELEMENT_FIELD = "element"


@node.handler
async def read(body: Body) -> Optional[Body]:
    """Process the read message."""
    return {
        BodyFields.TYPE: MessageTypes.READ_OK,
        VALUE_FIELD: gset.current_state(),
    } | reply_to(body)


@node.handler
async def add(body: Body) -> Optional[Body]:
    """Handle add messages."""
    data = body.get(ELEMENT_FIELD, None)
    if data is None:
        raise MessageError(ErrorType.BAD_REQ)
    gset.add(data)
    return {BodyFields.TYPE: MessageTypes.ADD_OK} | reply_to(body)


@node.handler
async def replicate(body: Body) -> Optional[Body]:
    """Update CRDT with replicated messages."""
    data = body.get(VALUE_FIELD, None)
    if data is None:
        raise MessageError(ErrorType.BAD_REQ)
    gset.update(data)
    return None


@node.worker_func
async def replicator():
    """Replicate elements."""
    while True:
        await asyncio.sleep(5)
        body = {
            BodyFields.TYPE: "replicate", VALUE_FIELD: gset.current_state()
        }
        tasks = [node.rpc(x, body) for x in node.dns()]
        await asyncio.gather(*tasks)


def main():
    node.run()


if __name__ == "__main__":
    main()
