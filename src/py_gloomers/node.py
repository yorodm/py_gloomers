"""Implementation of a Node and the RPC Stack."""
import asyncio
import sys
import json
import functools
from typing import Optional
from dataclasses import asdict
from py_gloomers.types import AbstractTransport, EventData, Body
from py_gloomers.types import MessageFields, MessageTypes, BodyFiels, \
    MessageError, ErrorType
from py_gloomers.types import Handler


__ERR_LOCK = asyncio.Lock()


async def log(message: str):
    """Log a message to stderr."""
    async with __ERR_LOCK:
        asyncio.get_event_loop().run_in_executor(
            None,
            functools.partial(print, message, file=sys.stderr, flush=True)
        )


class StdIOTransport(AbstractTransport):
    """A transport that uses stdio for communication."""

    connection_lost: asyncio.Event
    loop: asyncio.AbstractEventLoop
    output_lock: asyncio.Lock

    def __init__(self) -> None:
        """Initialize the transport."""
        self.connection_lost = asyncio.Event()
        self.output_lock = asyncio.Lock()

    async def connect(self, loop: asyncio.AbstractEventLoop):
        """Connect the transport."""
        self.loop = loop

    def connection_open(self) -> bool:
        """Return true if connection is still open."""
        return not self.connection_lost.is_set()

    async def read(self) -> Optional[str]:
        """Read data from the underlying connection."""
        line = await self.loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            self.connection_lost.set()
            return None
        return line

    async def send(self, data: EventData):
        """Send data to the underlying connection."""
        output = json.dumps(asdict(data))
        # It prevents us from making a mess out of stdout
        await log(f"Sending {output} to the network")
        async with self.output_lock:
            await self.loop.run_in_executor(None, lambda: print(output, flush=True))  # noqa


def reply_to(body: Body):
    """Return in reply to."""
    if body.get(BodyFiels.MSG_ID, False):
        return {BodyFiels.REPLY: body.get(BodyFiels.MSG_ID)}
    else:
        return {}


class Node:
    """Definition for the node."""

    transport: AbstractTransport
    handlers: dict[str, Handler]
    loop: asyncio.AbstractEventLoop
    node_id: Optional[str]  # Initially we have no name
    node_ids: list[str]
    message_count: int
    err_lock: asyncio.Lock

    def __init__(self, transport: AbstractTransport) -> None:
        """Create a node and set up its internal state."""
        self.transport = transport
        self.message_count = 0
        self.handlers = dict()
        # We are using this as a marker for the init status
        self.node_id = None
        self.err_lock = asyncio.Lock()

        async def init(body: Optional[Body]) -> Optional[Body]:
            """Handle init message from the network."""
            await log("Initializing node after init message")
            if self.node_id:
                pass  # Exception we've been initialized already
            if body is None:
                return None  # Exception, we need a body here
            self.node_id = body[BodyFiels.NODE_ID]
            self.node_ids = body.get(BodyFiels.NODE_IDS, [])
            return {
                BodyFiels.TYPE: MessageTypes.INIT_OK,
            } | reply_to(body)

        # Register init handler
        self.handler(init)

    async def start_serving(self):
        """Start the node server."""
        self.loop = asyncio.get_event_loop()
        await self.transport.connect(self.loop)
        while self.transport.connection_open():
            line = await self.transport.read()
            if line is None:
                continue  # Check the protocol docs
            try:
                data = json.loads(line)
                event = EventData(
                    data[MessageFields.SRC],
                    data[MessageFields.DEST],
                    data[MessageFields.BODY],  # noqa
                )
                await self.process_message(event)
            except MessageError as err:
                await self.emit(event.src, err.to_message())
            except KeyError:
                error = MessageError(ErrorType.BAD_REQ)
                await self.emit(event.src, error.to_message())

    async def emit(self, dest: str, body: Optional[Body]) -> None:
        """Emit a message back into the network."""
        if body is None:
            return None  # possibly an Exception
        if self.node_id is None:
            return None  # possibly another Exception
        self.message_count += 1
        body[BodyFiels.MSG_ID] = self.message_count
        event = EventData(self.node_id, dest, body)
        await self.transport.send(event)

    async def rpc(
        self,
        dest: str,
        message_type: MessageTypes,
        body: Body,
        callback: Handler,  # noqa
    ):
        """Make an rpc call and wait for a response."""

    def handler(self, func: Handler):
        """Register a handler for a given message."""
        self.handlers[func.__name__] = func

    async def process_message(self, event: EventData) -> None:
        """Call the handler of the given message."""
        if message_type := event.body.get(BodyFiels.TYPE, None):
            await log(f"Received message of type {message_type}")
            if message_type not in list(MessageTypes):
                raise MessageError(ErrorType.BAD_REQ)
            if func := self.handlers.get(message_type, None):
                response = await func(event.body)
                await self.emit(event.src, response)

    def run(self):
        """Run the node."""
        asyncio.run(self.start_serving())
