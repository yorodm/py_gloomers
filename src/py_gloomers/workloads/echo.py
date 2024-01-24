"""Echo module."""
from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body, \
    log, reply_to
from py_gloomers.types import BodyFiels, MessageTypes


node = Node(transport=StdIOTransport())

ECHO_FIELD = "echo"


@node.handler
async def echo(body: Body) -> Optional[Body]:
    """Worload for echo."""
    await log("Processing echo message")
    return {
        BodyFiels.TYPE: MessageTypes.ECHO_OK,
        ECHO_FIELD: body.get(ECHO_FIELD),
    } | reply_to(body)


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
