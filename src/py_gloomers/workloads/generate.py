"""Generate workload."""
import uuid
from typing import Optional
from py_gloomers.node import StdIOTransport, Node, Body, \
    log, reply_to
from py_gloomers.types import MessageTypes, BodyFields


node = Node(transport=StdIOTransport())


INPUT_FIELD = "id"


@node.handler
async def generate(body: Body) -> Optional[Body]:
    """Generate workload."""
    await log("Processing unique-ids message")
    return {
        BodyFields.TYPE: MessageTypes.GEN_OK,
        INPUT_FIELD: str(uuid.uuid4()),
    } | reply_to(body)


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
