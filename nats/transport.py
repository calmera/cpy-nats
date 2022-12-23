import asyncio
import sys
import ssl
from . import errors


class TcpTransport:

    def __init__(self):
        self._bare_io_reader = None
        self._io_reader = None
        self._bare_io_writer = None
        self._io_writer = None

    async def connect(
        self, uri, buffer_size: int, connect_timeout: int
    ):
        """
        Connects to a server using the implemented transport. The uri passed is of type ParseResult that can be
        obtained calling urllib.parse.urlparse.
        """
        r, w = await asyncio.wait_for(
            asyncio.open_connection(
                host=uri.hostname,
                port=uri.port,
                limit=buffer_size,
            ), connect_timeout
        )
        # We keep a reference to the initial transport we used when
        # establishing the connection in case we later upgrade to TLS
        # after getting the first INFO message. This is in order to
        # prevent the GC closing the socket after we send CONNECT
        # and replace the transport.
        #
        # See https://github.com/nats-io/asyncio-nats/issues/43
        self._bare_io_reader = self._io_reader = r
        self._bare_io_writer = self._io_writer = w

    async def connect_tls(
        self,
        uri,
        ssl_context: ssl.SSLContext,
        buffer_size: int,
        connect_timeout: int,
    ):
        """
        connect_tls is similar to connect except it tries to connect to a secure endpoint, using the provided ssl
        context. The uri can be provided as string in case the hostname differs from the uri hostname, in case it
        was provided as 'tls_hostname' on the options.
        """
        # loop.start_tls was introduced in python 3.7
        # the previous method is removed in 3.9
        if sys.version_info.minor >= 7:
            # manually recreate the stream reader/writer with a tls upgraded transport
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            transport_future = asyncio.get_running_loop().start_tls(
                self._io_writer.transport,
                protocol,
                ssl_context,
                # hostname here will be passed directly as string
                server_hostname=uri if isinstance(uri, str) else uri.hostname
            )
            transport = await asyncio.wait_for(
                transport_future, connect_timeout
            )
            writer = asyncio.StreamWriter(
                transport, protocol, reader, asyncio.get_running_loop()
            )
            self._io_reader, self._io_writer = reader, writer
        else:
            transport = self._io_writer.transport
            sock = transport.get_extra_info('socket')
            if not sock:
                # This shouldn't happen
                raise errors.Error('nats: unable to get socket')

            connection_future = asyncio.open_connection(
                limit=buffer_size,
                sock=sock,
                ssl=ssl_context,
                # hostname here will be passed directly as string
                server_hostname=uri if isinstance(uri, str) else uri.hostname,
            )
            self._io_reader, self._io_writer = await asyncio.wait_for(
                connection_future, connect_timeout
            )

    def write(self, payload):
        """
        Write bytes to underlying transport. Needs a call to drain() to be successfully written.
        """
        return self._io_writer.write(payload)

    def writelines(self, payload):
        """
        Writes a list of bytes, one by one, to the underlying transport. Needs a call to drain() to be successfully
        written.
        """
        return self._io_writer.writelines(payload)

    async def read(self, buffer_size: int):
        """
        Reads a sequence of bytes from the underlying transport, up to buffer_size. The buffer_size is ignored in case
        the transport carries already frames entire messages (i.e. websocket).
        """
        return await self._io_reader.read(buffer_size)

    async def readline(self):
        """
        Reads one whole frame of bytes (or message) from the underlying transport.
        """
        return await self._io_reader.readline()

    async def drain(self):
        """
        Flushes the bytes queued for transmission when calling write() and writelines().
        """
        return await self._io_writer.drain()

    async def wait_closed(self):
        """
        Waits until the connection is successfully closed.
        """
        return await self._io_writer.wait_closed()

    def close(self):
        """
        Closes the underlying transport.
        """
        return self._io_writer.close()

    def at_eof(self):
        """
        Returns if underlying transport is at eof.
        """
        return self._io_reader.at_eof()

    def __bool__(self):
        """
        Returns if the transport was initialized, either by calling connect of connect_tls.
        """
        return bool(self._io_writer) and bool(self._io_reader)
