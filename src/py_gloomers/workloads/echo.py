from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body
import asyncio

node = Node(transport=StdIOTransport())


async def start():
    """Entry point for echo."""
    await node.start_serving(asyncio.get_event_loop())


def main():
    """Enty point for script."""
    asyncio.run(start())


@node.handler
async def echo(body: Body) -> Optional[Body]:
    """Worload for echo."""
    return {
        "type": "echo_ok",
        "in_reply_to": body.get("msg_id"),
        "echo": body.get("echo"),
    }


if __name__ == "__main__":
    main()
