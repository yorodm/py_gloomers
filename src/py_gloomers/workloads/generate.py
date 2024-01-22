"""Generate workload."""
import asyncio
import uuid
from py_gloomers.node import StdIOTransport, Node, Body
from typing import Optional

node = Node(transport=StdIOTransport())


async def start():
    """Start the workload."""
    loop = asyncio.get_event_loop()
    await node.start_serving(loop)


@node.handler
async def generate(body: Body) -> Optional[Body]:
    """Generate workload."""
    return {
        "type": "generate_ok",
        "in_reply_to": body.get("msg_id"),
        "id": uuid.uuid4().int
    }


def main():
    """Entry point."""
    asyncio.run(start())


if __name__ == "__main__":
    main()
