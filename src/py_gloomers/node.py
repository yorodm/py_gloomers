"""Implementation of a Node and the RPC Stack."""
import asyncio
import sys
import json
import functools
from typing import Optional, Union, Awaitable
from dataclasses import asdict
from py_gloomers.types import AbstractTransport, EventData, Body
from py_gloomers.types import MessageFields, MessageTypes, BodyFields, \
    MessageError, ErrorType, WorkerFn
from py_gloomers.types import Handler, Worker, Timeout
import datetime


__ERR_LOCK = asyncio.Lock()


async def log(message: str):
    """Log a message to stderr."""
    async with __ERR_LOCK:
        message = f"{datetime.datetime.now()} -  {message}"
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
        self.loop = asyncio.get_event_loop()

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
    if body.get(BodyFields.MSG_ID, False):
        return {BodyFields.REPLY: body.get(BodyFields.MSG_ID)}
    return {}


class Node:
    """Definition for the node."""

    transport: AbstractTransport
    __handlers: dict[str, Handler]
    loop: asyncio.AbstractEventLoop
    node_id: Optional[str]  # Initially we have no name
    node_ids: set[str]
    message_count: int
    err_lock: asyncio.Lock
    callbacks: dict[int, asyncio.Future[Body]]
    workers: list[asyncio.Task]

    def __init__(self, transport: AbstractTransport) -> None:
        """Create a node and set up its internal state."""
        self.transport = transport
        self.message_count = 0
        self.__handlers = {}
        # We are using this as a marker for the init status
        self.node_id = None
        self.err_lock = asyncio.Lock()
        self.callbacks = {}
        self.workers: list[asyncio.Task] = []
        self.loop = asyncio.get_event_loop()
        self.node_ids = set()

        async def init(body: Optional[Body]) -> Optional[Body]:
            """Handle init message from the network."""
            await log("Initializing node after init message")
            if self.node_id:
                pass  # Exception we've been initialized already
            if body is None:
                return None  # Exception, we need a body here
            self.node_id = body[BodyFields.NODE_ID]
            self.node_ids.update(body.get(BodyFields.NODE_IDS, []))
            await log(f"Initialized {self.node_id} in a network with {self.node_ids}")  # noqa
            return {
                BodyFields.TYPE: MessageTypes.INIT_OK,
            } | reply_to(body)

        # Register init handler
        self.handler(init)

    async def start_serving(self):
        """Start the node server."""
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
                await log(f"Detected {err} while processing data")
                await self.emit(event.src, err.to_message())
                continue
            # except KeyError as e:
            #     await log(f"Ignoring malformed message {line} {e}")
            #     continue  # check the protocol docs
        for _, call in self.callbacks.items():
            call.cancel()
        for worker in self.workers:
            worker.cancel()

    async def emit(self, dest: str, body: Optional[Body]) -> None:
        """Emit a message back into the network."""
        if body is None:
            return None  # We have handlers that don't return body
        if self.node_id is None:
            return None  # possibly another Exception
        self.message_count += 1
        body[BodyFields.MSG_ID] = self.message_count
        event = EventData(self.node_id, dest, body)
        await self.transport.send(event)

    async def rpc(
        self,
        dest: str,
        body: Body,
    ) -> Union[Body, Timeout]:
        """Make an rpc call and wait for a response."""
        fut = self.loop.create_future()
        await self.emit(dest, body)
        index = self.message_count
        self.callbacks[index] = fut
        try:
            res = await asyncio.wait_for(fut, timeout=20)
            return res
        except TimeoutError:
            await log(f"Future from message_id {index} timed out")
            fut = self.callbacks.pop(index, None)
            if fut is None:
                await log(f"Response handler for {index} vanished")
            else:
                fut.cancel()
            return Timeout(body)
        except asyncio.exceptions.CancelledError:
            await log(f"Got cancell error for {body}")
            await log(f"State if callbacks is {self.callbacks}")
            return Timeout(body)

    def dns(self) -> set[str]:
        """
        Resolve the names of every host we received on init.

        The different between this and topology is that we
        don't care about partitions here.
        """
        return self.node_ids - {self.node_id}

    def add_worker(self, worker: Worker):
        """Add a worker task to run in the node."""
        # Do I need to keep track of these?
        new_worker = self.loop.create_task(worker)
        # Make sure we remove it after we are done
        # But what about the ones that run forever?
        new_worker.add_done_callback(self.workers.remove)
        self.workers.append(new_worker)

    def worker_func(self, fn: WorkerFn):
        """Add a worker to run when the node start."""
        self.add_worker(fn())

    def handler(self, func: Handler):
        """Register a handler for a given message."""
        self.__handlers[func.__name__] = func

    async def process_message(self, event: EventData) -> None:
        """Call the handler of the given message."""
        # Handle RPC messages
        await log(f"Got event ${event}")
        if (reply := event.body.get(BodyFields.REPLY, None)) is not None:
            await log(f"Event is in reply to {reply}")
            fut = self.callbacks.pop(reply, None)
            if fut is not None:
                fut.set_result(event.body)
                return
        # Handle regular messages
        if message_type := event.body.get(BodyFields.TYPE, None):
            await log(f"Received message of type {message_type}")
            if message_type not in list(MessageTypes):
                raise MessageError(ErrorType.BAD_REQ)
            if func := self.__handlers.get(message_type, None):
                response = await func(event.body)
                await self.emit(event.src, response)

    def run(self):
        """Run the node."""
        self.loop.run_until_complete(self.start_serving())


class KeyStores:
    """Proxy to access a service."""

    node: Node
    name: str

    def __init__(self, name: str, node: Node):
        """Initialize the service."""
        self.name = name
        self.node = node

    def call(self, body: Body) -> Awaitable[Union[Body | Timeout]]:
        """Pass a call to the service."""
        return self.node.rpc(self.name, body)

    def read(self, body: Body) -> Awaitable[Union[Body | Timeout]]:
        """Send a read call into a service."""
        body.update({
            BodyFields.TYPE: MessageTypes.READ
        })
        return self.call(body)

    def write(self, body: Body) -> Awaitable[Union[Body | Timeout]]:
        """Send a write call into a service."""
        body.update({
            BodyFields.TYPE: MessageTypes.WRITE
        })
        return self.call(body)

    def cas(self, body: Body) -> Awaitable[Union[Body | Timeout]]:
        """Send a compare and swap call into a service."""
        body.update({
            BodyFields.TYPE: MessageTypes.CAS
        })
        return self.call(body)
