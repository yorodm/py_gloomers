"""Echo module."""
from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body, \
    log, reply_to
from py_gloomers.types import BodyFields, MessageTypes


node = Node(transport=StdIOTransport())

INPUT_FIELD = "echo"


@node.handler
async def echo(body: Body) -> Optional[Body]:
    """Worload for echo."""
    await log("Processing echo message")
    return {
        BodyFields.TYPE: MessageTypes.ECHO_OK,
        INPUT_FIELD: body.get(INPUT_FIELD),
    } | reply_to(body)


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
