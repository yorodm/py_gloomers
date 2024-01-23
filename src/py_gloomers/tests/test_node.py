import io
import asyncio
import json
from dataclasses import asdict
from typing import Optional
from unittest.mock import patch
from unittest import IsolatedAsyncioTestCase
from py_gloomers.node import StdIOTransport, EventData, AbstractTransport



ECHO_MESSAGE = "{\"src\": \"c1\", \"dest\": \"n1\", \"body\": {\"type\": \"echo\", \"msg_id\": 1, \"echo\": \"Please echo 35\"}}" # noqa


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
        data: Optional[EventData] = await transport.read()
        # Then
        assert data is not None
        assert asdict(data) == json.loads(ECHO_MESSAGE)
        # When
        await transport.send(data)
        # Then
        assert stdout.getvalue().strip() == json.dumps(asdict(data))
        # When
        data = await transport.read()
        # Then
        assert data is None
        assert transport.connection_open() is False


class ListBasedTransport(AbstractTransport):
    pass


class TestNode(IsolatedAsyncioTestCase):
    pass
