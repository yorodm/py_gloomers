"""Workload for broadcast."""
import asyncio
from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body, log
from py_gloomers.types import BodyFiels, MessageTypes, MessageError, \
    ErrorType
from py_gloomers.node import reply_to


node = Node(transport=StdIOTransport())

ECHO_FIELD = "echo"


values: set[int] = set()
cluster: list[str] = []
INPUT_FIELD = "message"
REPLY_FIELD = "messages"
TOPOLOGY_FIELD = "topology"


@node.handler
async def broadcast(body: Body) -> Optional[Body]:
    """Worload for broadcast."""
    await log("Processing broadcast message")
    value = body.get(INPUT_FIELD, None)
    if value is None:
        raise MessageError(ErrorType.BAD_REQ)
    values.add(value)
    return {
        BodyFiels.TYPE: MessageTypes.BROAD_OK,
    } | reply_to(body)


@node.handler
async def read(body: Body) -> Optional[Body]:
    """Part of the broadcast workload."""
    await log("Processing read message")
    return {
        BodyFiels.TYPE: MessageTypes.READ_OK,
        REPLY_FIELD: list(values)
    } | reply_to(body)


@node.handler
async def topology(body: Body) -> Optional[Body]:
    """Part of the broadcast workload."""
    await log("Processing topology")
    if network := body.get(TOPOLOGY_FIELD, False):
        cluster: list[str] = network.get(node.node_id, [])
        for member in cluster:
            node.add_worker(gossiper(member))
    else:
        raise MessageError(ErrorType.BAD_REQ)
    return {
        BodyFiels.TYPE: MessageTypes.TOPOLOGY_OK
    } | reply_to(body)



async def gossiper(dest: str):
    """Send data to a node."""
    sent = set()
    while True:
        # 1. See if we have new values to send.
        # 2. Rpc message with values.
        # 3. If rpc was a success mark values as sent (add to the set)
        # 4. wait for event saying we have new values

        pass


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
