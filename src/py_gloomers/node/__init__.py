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


def dataclass_from_dict(klass: Any, d: dict) -> Any:
    """Convert dict into a dataclass."""
    try:
        fieldtypes = {f.name: f.type for f in fields(klass)}
        return klass(**{f: dataclass_from_dict(fieldtypes[f], d[f]) for f in d})
    except Exception:
        raise MessageError()


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


Handler: TypeAlias = Callable[[EventData], Optional[Body]]


@dataclass
class Node:
    """Definition for the node."""

    transport: AbstractTransport
    handlers: dict[str, Handler]
    loop: asyncio.AbstractEventLoop

    async def start_serving(self):
        """Start the node server."""
        while self.transport.connection_open():
            data = await self.transport.read()
            self.handle(data)

    def emit(self, body: Optional[Body]):
        """Emit a message back into the network."""

    def handler(self, func: Handler):
        """Register a handler for a given message."""

    def handle(self, data: dict) -> None:
        """Call the handler of the given message."""
        event = dataclass_from_dict(EventData, data)
        if marker := event.body.get("type", None):
            if handler := self.handlers.get(marker, None):
                response = handler(event.body)
                self.emit(response)
