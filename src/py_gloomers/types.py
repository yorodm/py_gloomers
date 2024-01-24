"""Common types."""
import asyncio
from typing import TypeAlias, Any, Optional
from typing import Callable, Awaitable
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import StrEnum

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


class MessageTypes(StrEnum):
    """Node message types."""

    ECHO = "echo"
    ECHO_OK = "echo_ok"
    INIT = "init"
    INIT_OK = "init_ok"
    GENERATE = "generate"
    GENERATE_OK = "generate_ok"


class MessageFields(StrEnum):
    """Important fields in the message wrapper."""

    DEST = "dest"
    SRC = "src"
    BODY = "body"


class BodyFiels(StrEnum):
    """Important fields in message body."""

    TYPE = "type"
    REPLY = "in_reply_to"
    NODE_ID = "node_id"
    NODE_IDS = "node_ids"
    MSG_ID = "msg_id"


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


Handler: TypeAlias = Callable[[Body], Awaitable[Optional[Body]]]
