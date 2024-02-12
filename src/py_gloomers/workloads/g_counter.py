"""Workload for broadcast."""
import asyncio
from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body, Service
from py_gloomers.types import (
    BodyFields,
    MessageTypes,
    ErrorType,
    Timeout,
    is_error,
)
from py_gloomers.node import reply_to


node = Node(transport=StdIOTransport())
KEY_NAME = "g_counter"


class GCounter:
    """The GCounter workload."""

    service: Service
    __counter: int

    def __init__(self):
        """Initialize the workload."""
        self.service = Service("seq-kv", node)
        self.__counter = 0

    async def proxy_read(self):
        """Proxy the call and update."""
        response = await self.service.call({
            BodyFields.TYPE: MessageTypes.READ,
            "key": KEY_NAME,
        })
        if isinstance(response, Timeout):
            return self.__counter
        if is_error(response, ErrorType.NOT_FOUND_KEY):
            return self.__counter
        if response.get(BodyFields.VALUE, None) is not None:
            self.__counter = response.get(BodyFields.VALUE)
        return self.__counter

    async def add(self, delta: int):
        """Increase the counter."""
        # 1. check and swap the increment in the linkv
        # 2. if we receive an error read and update
        #    a) There is no data so we should call write
        #    b) There is data so we should call read
        self.__counter += delta
        response = await self.service.call(
            {
                BodyFields.TYPE: MessageTypes.CAS,
                "from": self.__counter,
                "to": self.__counter+delta,
                "key": "g_counter",
            }
        )
        if isinstance(response, Timeout):
            # If i want to work with network partitions there should be
            # a retry strategy of some kind.
            return  # Let's keep the typcheck happy
        if response.get(BodyFields.TYPE, "") == MessageTypes.CAS_OK.value:
            self.__counter += delta
            return
        if is_error(response, ErrorType.COND_FAILED):
            await self.sync_counter(delta)
            return
        if is_error(response, ErrorType.NOT_FOUND_KEY):
            await self.write(self.__counter+delta)
            return

    async def write(self, value: int):
        """Write a value to the lib-kv."""
        response = await self.service.call({
            BodyFields.TYPE: MessageTypes.WRITE,
            "key": KEY_NAME,
            BodyFields.VALUE: value
        })
        if isinstance(response, Timeout):
            return  # again no retry strategy
        self.__counter = value  # cheap trick

    async def sync_counter(self, delta: int):
        """Swap the counter for the value in the lin-kv."""
        # 1. Read from linkv
        response = await self.service.call({
            BodyFields.TYPE: MessageTypes.READ,
            "key": KEY_NAME,
        })
        if isinstance(response, Timeout):
            return  # again no retry strategy
        if (value := response.get(BodyFields.VALUE, None)) is not None:
            # 2. If we have a higher value write
            if value < (self.__counter + delta):
                await self.write(self.__counter + delta)
                return
            # 3. Else we swap for whatever linkv has
            self.__counter = value


g_counter = GCounter()


@node.handler
async def read(body: Body) -> Optional[Body]:
    """Handle read message."""
    data = await g_counter.proxy_read()
    return {
        BodyFields.TYPE: MessageTypes.READ_OK,
        BodyFields.VALUE: data
    } | reply_to(body)


@node.handler
async def add(body: Body) -> Optional[Body]:
    """Handle add message."""
    if value := body.get(BodyFields.DELTA, None) is not None:
        # Fire and forget this
        asyncio.create_task(g_counter.add(value))
    return {
        BodyFields.TYPE: MessageTypes.ADD_OK
    } | reply_to(body)


def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
