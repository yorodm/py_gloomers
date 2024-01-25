import io
import asyncio
import json
from dataclasses import asdict
from typing import Optional, Any
from unittest.mock import patch
from unittest import IsolatedAsyncioTestCase
from py_gloomers.node import StdIOTransport, Node
from py_gloomers.types import AbstractTransport, EventData
from py_gloomers.types import MessageFields, BodyFiels, MessageTypes



ECHO_MESSAGE = "{\"src\": \"c1\", \"dest\": \"n1\", \"body\": {\"type\": \"echo\", \"msg_id\": 1, \"echo\": \"Please echo 35\"}}" # noqa
INIT_MESSAGE = "{ \"src\":\"c0\", \"dest\": \"n1\", \"body\":{\"type\":\"init\", \"msg_id\":1,\"node_id\":  \"n3\",\"node_ids\": [\"n1\", \"n2\", \"n3\"]}}" # noqa


class TestTransport(IsolatedAsyncioTestCase):
    """Test the StdIoTransport."""

    @patch("sys.stdin", io.StringIO(ECHO_MESSAGE))
    @patch("sys.stdout", new_callable=io.StringIO)
    async def test_read(self, stdout: io.StringIO) -> None:
        # Given
        loop = asyncio.get_event_loop()
        transport = StdIOTransport()
        await transport.connect(loop)
        # When
        data: Optional[str] = await transport.read()
        # Then
        self.assertIsNotNone(data)
        serialized = json.loads(data)  # ignore this warning
        self.assertEqual(serialized, json.loads(ECHO_MESSAGE))
        # When
        event = EventData(
            serialized[MessageFields.SRC],
            serialized[MessageFields.DEST],
            serialized[MessageFields.BODY],  # noqa
        )
        await transport.send(event)
        # Then
        self.assertEqual(stdout.getvalue().strip(), json.dumps(asdict(event)))
        # When
        data = await transport.read()
        # Then
        self.assertIsNone(data)
        self.assertFalse(transport.connection_open())


class ListBasedTransport(AbstractTransport):

    input_buffer: list[str]
    output_buffer: list[dict[str, Any]]

    def __init__(self, input_data: list[str]):
        self.input_buffer = input_data
        self.output_buffer = []

    async def send(self, data: EventData) -> None:
        self.output_buffer.append(asdict(data))

    async def connect(self, _: asyncio.AbstractEventLoop) -> None:
        pass

    async def read(self) -> Optional[str]:
        if len(self.input_buffer):
            return self.input_buffer.pop(0)
        return None

    def connection_open(self) -> bool:
        return len(self.input_buffer) != 0


class EventBasedTransport(ListBasedTransport):

    event: asyncio.Event

    def __init__(self, input_data: list[str]):
        super().__init__(input_data)
        self.event = asyncio.Event()

    async def set(self):
        # Yes I know, but we need to run this in a TaskGroup
        self.event.set()

    async def read(self) -> Optional[str]:
        await self.event.wait()
        value = await super().read()
        self.event.clear()
        return value


class TestNode(IsolatedAsyncioTestCase):

    async def test_init(self) -> None:
        # Given
        input_data = [
            INIT_MESSAGE
        ]
        transport = ListBasedTransport(input_data)
        node = Node(transport=transport)
        # When
        await node.start_serving()
        # Then
        response = transport.output_buffer.pop(0)
        # We respon with init_ok
        self.assertEqual(response[MessageFields.BODY][BodyFiels.TYPE], MessageTypes.INIT_OK)  # noqa
        # The node is initialized
        self.assertEqual(node.node_id, "n3")

    async def test_malformed(self) -> None:
        # Given
        input_data = [
            INIT_MESSAGE,
            "{}"
        ]
        transport = ListBasedTransport(input_data)
        node = Node(transport=transport)
        # When
        await node.start_serving()
        self.assertEqual(len(transport.output_buffer), 1)

    async def test_rpc(self):
        rpc_send = {
            "type": "rpc_send"
        }
        rpc_reply = {
            MessageFields.SRC: "c0",
            MessageFields.DEST: "n1",
            MessageFields.BODY: {
                BodyFiels.TYPE: "rpc_reply",
                BodyFiels.REPLY: 2
            }
        }
        input_data = [
            INIT_MESSAGE,
            json.dumps(rpc_reply)
        ]
        transport = EventBasedTransport(input_data)
        await transport.set()
        node = Node(transport=transport)
        async with asyncio.TaskGroup() as group:
            group.create_task(node.start_serving())
            rpc_response = group.create_task(node.rpc("c0", rpc_send))
            group.create_task(transport.set())
        self.assertEqual(len(node.callbacks), 0)
        self.assertEqual(rpc_response.result(), rpc_reply[MessageFields.BODY])

    async def test_workers(self):
        fut = asyncio.get_event_loop().create_future()

        async def set_fut(value: int):
            fut.set_result(value)

        input_data = [
            INIT_MESSAGE
        ]
        transport = EventBasedTransport(input_data)
        node = Node(transport=transport)
        async with asyncio.TaskGroup() as group:
            group.create_task(node.start_serving())
            group.create_task(transport.set())
            node.add_worker(set_fut(10))
        self.assertEqual(await fut, 10)
        self.assertTrue(len(node.workers) == 0)
