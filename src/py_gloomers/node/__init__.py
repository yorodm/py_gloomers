"""Implementation of a Node and the RPC Stack."""
from typing import TypeAlias, Any, Callable, Optional
from dataclasses import dataclass, fields, asdict
from abc import ABC, abstractmethod
import asyncio
import asyncio.streams
import sys
import json

Body: TypeAlias = dict[str, Any]


class MessageError(Exception):
    """Raise if a message was not valid."""

    pass


@dataclass
class EventData:
    """The data for received events."""

    src: str
    dst: str
    body: Body


class AbstractTransport(ABC):
    """Basic interface for a transport."""

    @abstractmethod
    async def send(self, data: EventData) -> None:
        """Send data using this transport."""

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

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Initialize the transport."""
        self.connection_lost = asyncio.Event()
        self.loop = loop
        self.output_lock = asyncio.Lock()

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
            await self.loop.run_in_executor(None,
                                            lambda: print(output, flush=True))


Handler: TypeAlias = Callable[[Body], Optional[Body]]


@dataclass
class Node:
    """Definition for the node."""

    transport: AbstractTransport
    handlers: dict[str, Handler]
    loop: asyncio.AbstractEventLoop
    node_id: Optional[str]  # Initially we have no name
    node_ids: list[str]
    message_count: int

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 transport: AbstractTransport) -> None:
        """Create a node and set up its internal state."""
        self.loop = loop
        self.transport = transport
        self.message_count = 0
        self.name = None  # We are using this as a marker for the init status

        def init(body: Optional[Body]) -> Optional[Body]:
            """Handle init message from the network."""
            if self.node_id:
                pass  # Exception we've been initialized already
            if body is None:
                return None  # Exception, we need a body here
            self.node_id = body["node_id"]
            self.node_ids = body.get("node_ids", [])
            return {"type": "init_ok", "in_reply_to": body["msg_id"]}
        # Register init handler
        self.handler(init)

    async def start_serving(self):
        """Start the node server."""
        while self.transport.connection_open():
            data = await self.transport.read()
            self.handle(data)

    def emit(self, dest: str, body: Optional[Body]) -> Optional[asyncio.Handle]:
        """Emit a message back into the network."""
        if body is None:
            return None  # possibly an Exception
        if self.node_id is None:
            return None  # possibly another Exception
        self.message_count += 1
        body["msg_id"] = self.message_count
        event = EventData(self.node_id, dest, body)
        return self.loop.call_soon(self.transport.send, event)

    def handler(self, func: Handler):
        """Register a handler for a given message."""
        self.handlers[func.__name__] = func

    def handle(self, data: dict) -> None:
        """Call the handler of the given message."""
        if self.node_id is None:
            return  # Something came here before init
        event = EventData(data["src"], data["dest"], data["body"])
        if marker := event.body.get("type", None):
            if func := self.handlers.get(marker, None):
                response = func(event.body)
                self.emit(event.dst, response)
