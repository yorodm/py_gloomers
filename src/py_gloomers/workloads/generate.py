"""Generate workload."""
import uuid
from typing import Optional
from py_gloomers.node import StdIOTransport, Node, Body, log
from py_gloomers.types import MessageTypes, BodyFiels
from .support import run

node = Node(transport=StdIOTransport())


ID_FIELD = "id"


@node.handler
async def generate(body: Body) -> Optional[Body]:
    """Generate workload."""
    await log("Processing unique-ids message")
    reply = (
        {BodyFiels.REPLY: body.get(BodyFiels.MSG_ID)}
        if body.get(BodyFiels.MSG_ID, False)
        else {}
    )
    return {
        BodyFiels.TYPE: MessageTypes.GEN_OK,
        ID_FIELD: str(uuid.uuid4()),
    } | reply


def main():  # noqa
    run(node)


if __name__ == "__main__":
    main()
