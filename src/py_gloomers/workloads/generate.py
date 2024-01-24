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
    return {
        BodyFiels.TYPE: MessageTypes.GENERATE_OK,
        # Remember this are optional
        BodyFiels.MSG_ID: body.get(BodyFiels.MSG_ID),
        ID_FIELD: str(uuid.uuid4()),
    }


def main():
    run(node)


if __name__ == "__main__":
    main()
