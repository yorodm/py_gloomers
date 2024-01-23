import asyncio
from py_gloomers.node import Node


async def start(node: Node):
    """Entry point for echo."""
    await node.start_serving(asyncio.get_event_loop())


def main(node: Node):
    """Enty point for script."""
    asyncio.run(start(node))
