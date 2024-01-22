"""Implementation of a Node and the RPC Stack."""
from typing import TypeAlias, Any, Callable, Optional, Awaitable
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import asyncio
import sys
import json
import functools

Body: TypeAlias = dict[str, Any]


class MessageError(Exception):
    """Raise if a message was not valid."""

    pass


@dataclass
class EventData:
    """The data for received events."""

    src: str
    dest: str
    body: Body


class AbstractTransport(ABC):
    """Basic interface for a transport."""

    @abstractmethod
    async def send(self, data: EventData) -> None:
        """Send data using this transport."""

    @abstractmethod
    async def connect(self, loop: asyncio.AbstractEventLoop) -> None:
        """Connect the transport."""

    @abstractmethod
    async def read(self) -> Optional[EventData]:
        """Receive data using this transport."""

    @abstractmethod
    def connection_open(self) -> bool:
        """Return true if the connection is still open."""


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

    async def read(self) -> Optional[EventData]:
        """Read data from the underlying connection."""
        line = await self.loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            self.connection_lost.set()
            return None
        data = json.loads(line.strip())
        return EventData(data["src"], data["dest"], data["body"])

    async def send(self, data: EventData):
        """Send data to the underlying connection."""
        output = json.dumps(asdict(data))
        # It prevents us from making a mess out of stdout
        async with self.output_lock:
            await self.loop.run_in_executor(None, lambda: print(output, flush=True))


Handler: TypeAlias = Callable[[Body], Awaitable[Optional[Body]]]


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
        self.node_id = None  # We are using this as a marker for the init status
        self.err_lock = asyncio.Lock()

        async def init(body: Optional[Body]) -> Optional[Body]:
            """Handle init message from the network."""
            await self.log("Entering init")
            if self.node_id:
                pass  # Exception we've been initialized already
            if body is None:
                return None  # Exception, we need a body here
            self.node_id = body["node_id"]
            self.node_ids = body.get("node_ids", [])
            return {"type": "init_ok", "in_reply_to": body["msg_id"]}

        # Register init handler
        self.handler(init)

    async def log(self, message: str):
        """Log a message to stderr."""
        async with self.err_lock:
            self.loop.call_soon(
                functools.partial(print, message, file=sys.stderr, flush=True)
            )

    async def start_serving(self, loop: asyncio.AbstractEventLoop):
        """Start the node server."""
        self.loop = loop
        await self.transport.connect(self.loop)
        while self.transport.connection_open():
            data = await self.transport.read()
            if data is None:
                continue  # Check the protocol docs
            await self.process_message(data)

    async def emit(self, dest: str, body: Optional[Body]) -> None:
        """Emit a message back into the network."""
        await self.log("Entering emit")
        if body is None:
            return None  # possibly an Exception
        if self.node_id is None:
            return None  # possibly another Exception
        self.message_count += 1
        body["msg_id"] = self.message_count
        event = EventData(self.node_id, dest, body)
        await self.transport.send(event)

    def handler(self, func: Handler):
        """Register a handler for a given message."""
        self.handlers[func.__name__] = func

    async def process_message(self, event: EventData) -> None:
        """Call the handler of the given message."""
        await self.log("Entering handle")
        if marker := event.body.get("type", None):
            await self.log(f"marker is {marker}")
            await self.log(f"handlers are {self.handlers}")
            if func := self.handlers.get(marker, None):
                response = await func(event.body)
                await self.log(f"response is {response}")
                await self.emit(event.src, response)
