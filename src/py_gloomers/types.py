"""Common types."""
import asyncio
from typing import TypeAlias, Any, Optional
from typing import Callable, Awaitable
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import StrEnum, IntEnum

Body: TypeAlias = dict[str, Any]


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
    GEN = "generate"
    GEN_OK = "generate_ok"
    BROAD = "broadcast"
    BROAD_OK = "broadcast_ok"
    ERROR = "error"


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


class ErrorType(IntEnum):
    """Error codes supported by the test suite."""

    TIMEOUT = 0
    NOT_FOUND = 1
    NOT_SUPPORTED = 10
    UNAVAILABLE = 11
    BAD_REQ = 12
    CRASH = 13
    ABORT = 14
    NOT_FOUND_KEY = 20
    DUPLICATE_KEY = 21
    COND_FAILED = 22
    CONFLICT = 30

    def is_definite(self) -> bool:
        """Check if error code is definite."""
        return False


class MessageError(Exception):
    """Raise if a message was not valid."""

    error: ErrorType
    reply: Optional[str]

    def __init__(self, error: ErrorType, reply: Optional[str] = None):
        """Create and exception wrapping the error code."""
        self.error = error
        self.reply = reply

    def to_message(self):
        """Turn this into an error message."""
        return {
            BodyFiels.TYPE: MessageTypes.ERROR, "code": self.error.value
        } | (
            {BodyFiels.REPLY: self.reply} if self.reply is not None else {}
        )


class AbstractTransport(ABC):
    """Basic interface for a transport."""

    @abstractmethod
    async def send(self, data: EventData) -> None:
        """Send data using this transport."""

    @abstractmethod
    async def connect(self, loop: asyncio.AbstractEventLoop) -> None:
        """Connect the transport."""

    @abstractmethod
    async def read(self) -> Optional[str]:
        """Receive data using this transport."""

    @abstractmethod
    def connection_open(self) -> bool:
        """Return true if the connection is still open."""


Handler: TypeAlias = Callable[[Body], Awaitable[Optional[Body]]]
