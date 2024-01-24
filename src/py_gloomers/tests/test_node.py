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
        assert data is not None
        serialized = json.loads(data)
        assert serialized == json.loads(ECHO_MESSAGE)
        # When
        event = EventData(
            serialized[MessageFields.SRC],
            serialized[MessageFields.DEST],
            serialized[MessageFields.BODY],  # noqa
        )
        await transport.send(event)
        # Then
        assert stdout.getvalue().strip() == json.dumps(asdict(event))
        # When
        data = await transport.read()
        # Then
        assert data is None
        assert transport.connection_open() is False


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
        assert response[MessageFields.BODY][BodyFiels.TYPE] == MessageTypes.INIT_OK  # noqa
        # The node is initialized
        assert node.node_id == "n3"

    async def test_error(self) -> None:
        # Given
        input_data = [
            INIT_MESSAGE,
            "{}"
        ]
        transport = ListBasedTransport(input_data)
        node = Node(transport=transport)
        # When
        await node.start_serving()
        response = transport.output_buffer[1]
        assert response[MessageFields.BODY][BodyFiels.TYPE] == MessageTypes.ERROR  # noqa
