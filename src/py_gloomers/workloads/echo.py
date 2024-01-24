"""Echo module."""
import enum
from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body, log
from py_gloomers.types import BodyFiels, MessageTypes
from .support import run


node = Node(transport=StdIOTransport())

ECHO_FIELD = "echo"


@node.handler
async def echo(body: Body) -> Optional[Body]:
    """Worload for echo."""
    await log("Processing echo message")
    return {
        BodyFiels.TYPE: MessageTypes.ECHO_OK,
        # This is optional
        BodyFiels.REPLY: body.get(BodyFiels.MSG_ID),
        ECHO_FIELD: body.get(ECHO_FIELD),
    }


def main():
    run(node)


if __name__ == "__main__":
    main()
