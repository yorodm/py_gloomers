"""Workload for broadcast."""
import asyncio
from typing import Optional, Coroutine
from py_gloomers.node import Node, StdIOTransport, Body
from py_gloomers.types import BodyFields, MessageTypes, MessageError, ErrorType
from py_gloomers.node import reply_to


node = Node(transport=StdIOTransport())

values: set[int] = set()
run_condition: asyncio.Condition = asyncio.Condition()
INPUT_FIELD = "message"
REPLY_FIELD = "messages"
TOPOLOGY_FIELD = "topology"


@node.handler
async def broadcast(body: Body) -> Optional[Body]:
    """Worload for broadcast."""
    value = body.get(INPUT_FIELD, None)
    if value is None:
        raise MessageError(ErrorType.BAD_REQ)
    if value not in values:
        values.add(value)
        async with run_condition:
            run_condition.notify_all()
    return {
        BodyFields.TYPE: MessageTypes.BROAD_OK,
    } | reply_to(body)


@node.handler
async def read(body: Body) -> Optional[Body]:
    """Part of the broadcast workload."""
    return {
        BodyFields.TYPE: MessageTypes.READ_OK,
        REPLY_FIELD: list(values),
    } | reply_to(body)


@node.handler
async def topology(body: Body) -> Optional[Body]:
    """Part of the broadcast workload."""
    if network := body.get(TOPOLOGY_FIELD, False):
        cluster: list[str] = network.get(node.node_id, [])
        for member in cluster:
            node.add_worker(gossiper(member))
    else:
        raise MessageError(ErrorType.BAD_REQ)
    return {BodyFields.TYPE: MessageTypes.TOPOLOGY_OK} | reply_to(body)


async def gossiper(dest: str):
    """Send data to a node."""
    sent = set()

    def create_broadcast(v: int) -> Body:
        return {BodyFields.TYPE: MessageTypes.BROAD, INPUT_FIELD: v}

    def annotate(w: Coroutine, value: int) -> asyncio.Task:
        task = asyncio.create_task(w)

        def update_sent(t: asyncio.Task):
            if isinstance(t.result(), dict):
                sent.add(value)

        task.add_done_callback(update_sent)
        return task

    while True:
        async with run_condition:
            await run_condition.wait()
            # 1. We have new data to send (calculate it)
            # 2. Rpc message with values.
            # 3. If rpc was a success mark values as sent (add to the set)
            # 4. wait for event saying we have new values
            new_data = values - sent
            tasks = [annotate(node.rpc(dest, create_broadcast(v)), v) for v in new_data]  # noqa
            # In case one of these tasks fails (rpc returns Timeout)
            # the corresponding value will not be added to the set so
            # it should be retried on the next notification
            asyncio.gather(*tasks)  # we don't care about results


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
