"""Core connection objects."""

from __future__ import annotations

import abc
import copy
import functools
import logging
import math
import platform
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Sequence,
    cast,
)

import pika.callback
import pika.channel
import pika.heartbeat
from pika import exceptions, frame, spec, validators
from pika.parameters import ConnectionParameters, Parameters

if TYPE_CHECKING:
    from pika import amqp_object
    from pika.channel import Channel

PRODUCT = 'Pika Python Client Library'

LOGGER = logging.getLogger(__name__)


class Connection(abc.ABC):
    """
    This is the core class that implements communication with RabbitMQ.

    This class should not be invoked directly but rather through the use of an adapter such as
    SelectConnection or BlockingConnection.
    """

    ON_CONNECTION_CLOSED = '_on_connection_closed'
    ON_CONNECTION_ERROR = '_on_connection_error'
    ON_CONNECTION_OPEN_OK = '_on_connection_open_ok'
    CONNECTION_CLOSED = 0
    CONNECTION_INIT = 1
    CONNECTION_PROTOCOL = 2
    CONNECTION_START = 3
    CONNECTION_TUNE = 4
    CONNECTION_OPEN = 5
    CONNECTION_CLOSING = 6  # client-initiated close in progress

    _STATE_NAMES: ClassVar[dict[int, str]] = {
        CONNECTION_CLOSED: 'CLOSED',
        CONNECTION_INIT: 'INIT',
        CONNECTION_PROTOCOL: 'PROTOCOL',
        CONNECTION_START: 'START',
        CONNECTION_TUNE: 'TUNE',
        CONNECTION_OPEN: 'OPEN',
        CONNECTION_CLOSING: 'CLOSING'
    }

    def __init__(self,
                 parameters: Parameters | None = None,
                 on_open_callback: Callable[[Connection], Any] | None = None,
                 on_open_error_callback: Callable[[Connection, Exception],
                                                  Any] | None = None,
                 on_close_callback: Callable[[Connection, Exception], Any] |
                 None = None,
                 internal_connection_workflow: bool = True) -> None:
        """
        Connection initialization expects an object that has implemented the Parameters class and a
        callback function to notify when we have successfully connected to the AMQP Broker.

        Available Parameters classes are the ConnectionParameters class and
        URLParameters class.

        :param parameters: Read-only connection parameters.
        :param on_open_callback: Called when the connection is opened:
            on_open_callback(connection)
        :param on_open_error_callback: Callback (or None) with signature
            ``(Connection, Exception) -> Any``; called if the connection
            can't be established or connection establishment is interrupted by
            `Connection.close()`: on_open_error_callback(Connection, exception).
        :param on_close_callback: Callback (or None) with signature
            ``(Connection, Exception) -> Any``; called when a previously fully
            open connection is closed:
            `on_close_callback(Connection, exception)`, where `exception` is
            either an instance of `exceptions.ConnectionClosed` if closed by
            user or broker or exception of another type that describes the cause
            of connection failure.
        :param internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory.
        """
        self.connection_state: int = self.CONNECTION_CLOSED

        # Determines whether we invoke the on_open_error_callback or
        # on_close_callback. So that we don't lose track when state transitions
        # to CONNECTION_CLOSING as the result of Connection.close() call during
        # opening.
        self._opened: bool = False

        # Value to pass to on_open_error_callback or on_close_callback when
        # connection fails to be established or becomes closed
        self._error: Exception | None = None

        # Used to hold timer if configured for Connection.Blocked timeout
        self._blocked_conn_timer: object = None

        self._heartbeat_checker: pika.heartbeat.HeartbeatChecker | None = None

        # Set our configuration options
        if parameters is not None:
            # NOTE: Work around inability to copy ssl.SSLContext contained in
            # our SSLOptions; ssl.SSLContext fails to implement __getnewargs__
            saved_ssl_options = parameters.ssl_options
            parameters.ssl_options = None
            try:
                self.params = copy.deepcopy(parameters)
                self.params.ssl_options = saved_ssl_options
            finally:
                parameters.ssl_options = saved_ssl_options
        else:
            self.params = ConnectionParameters()

        self._internal_connection_workflow = internal_connection_workflow

        # Define our callback dictionary
        self.callbacks = pika.callback.CallbackManager()

        # Attributes that will be properly initialized by _init_connection_state
        # and/or during connection handshake.
        self.server_capabilities: dict[str, bool] | None = None
        self.server_properties: dict[str, Any] | None = None
        self._body_max_length: int = (spec.FRAME_MAX_SIZE -
                                      spec.FRAME_HEADER_SIZE -
                                      spec.FRAME_END_SIZE)
        self.known_hosts: str | bytes | None = None
        self._frame_buffer: bytearray = bytearray()
        self._processing_frame_buffer: bool = False
        self._channels: dict[int, Channel] = {}

        self._init_connection_state()

        # Add the on connection error callback
        self.callbacks.add(
            0, self.ON_CONNECTION_ERROR, on_open_error_callback or
            self._default_on_connection_error, False)

        # On connection callback
        if on_open_callback:
            self.add_on_open_callback(on_open_callback)

        # On connection callback
        if on_close_callback:
            self.add_on_close_callback(on_close_callback)

        self._set_connection_state(self.CONNECTION_INIT)

        if self._internal_connection_workflow:
            # Kick off full-stack connection establishment. It will complete
            # asynchronously.
            self._adapter_connect_stream()
        else:
            # Externally-managed connection workflow will proceed asynchronously
            # using adapter-specific mechanism
            LOGGER.debug('Using external connection workflow.')

    def _init_connection_state(self) -> None:
        """
        Initialize or reset all of the internal state variables for a given connection.

        On disconnect or reconnect all of the state needs to be wiped.
        """
        # TODO: probably don't need the state recovery logic since we don't
        #       test re-connection sufficiently (if at all), and users should
        #       just create a new instance of Connection when needed.
        # So, just merge the pertinent logic into the constructor.

        # Connection state
        self._set_connection_state(self.CONNECTION_CLOSED)

        # Negotiated server properties
        self.server_properties = None

        # Inbound buffer for decoding frames
        self._frame_buffer = bytearray()
        self._processing_frame_buffer = False

        # Dict of open channels
        self._channels = {}

        # Data used for Heartbeat checking
        self.bytes_sent = 0
        self.bytes_received = 0
        self.frames_sent = 0
        self.frames_received = 0
        self._heartbeat_checker = None

        # When closing, holds reason why
        self._error = None

        # Our starting point once connected, first frame received
        self._add_connection_start_callback()

        # Add a callback handler for the Broker telling us to disconnect.
        # NOTE: As of RabbitMQ 3.6.0, RabbitMQ broker may send Connection.Close
        # to signal error during connection setup (and wait a longish time
        # before closing the TCP/IP stream). Earlier RabbitMQ versions
        # simply closed the TCP/IP stream.
        self.callbacks.add(0, spec.Connection.Close,
                           self._on_connection_close_from_broker)

        if self.params.blocked_connection_timeout is not None:
            if self._blocked_conn_timer is not None:
                # Blocked connection timer was active when teardown was
                # initiated
                self._adapter_remove_timeout(self._blocked_conn_timer)
                self._blocked_conn_timer = None

            self.add_on_connection_blocked_callback(self._on_connection_blocked)
            self.add_on_connection_unblocked_callback(
                self._on_connection_unblocked)

    def add_on_close_callback(
            self, callback: Callable[[Connection, Exception], Any]) -> None:
        """
        Add a callback notification when the connection has closed.

        The callback will be passed the connection and an exception instance. The exception will
        either be an instance of `exceptions.ConnectionClosed` if a fully-open connection was closed
        by user or broker or exception of another type that describes the cause of connection
        closure/failure.

        :param callback: Callback to call on close, having the signature:
            callback(pika.connection.Connection, exception)
        """
        validators.require_callback(callback)
        self.callbacks.add(0, self.ON_CONNECTION_CLOSED, callback, False)

    def add_on_connection_blocked_callback(
        self,
        callback: Callable[[Connection, frame.Method[spec.Connection.Blocked]],
                           Any]
    ) -> None:
        """RabbitMQ AMQP extension - Add a callback to be notified when the
        connection gets blocked (`Connection.Blocked` received from RabbitMQ)
        due to the broker running low on resources (memory or disk). In this
        state RabbitMQ suspends processing incoming data until the connection
        is unblocked, so it's a good idea for publishers receiving this
        notification to suspend publishing until the connection becomes
        unblocked.

        See also `Connection.add_on_connection_unblocked_callback()`

        See also `ConnectionParameters.blocked_connection_timeout`.

        :param callback: Callback to call on `Connection.Blocked`,
            having the signature `callback(connection, pika.frame.Method)`,
            where the method frame's `method` member is of type
            `pika.spec.Connection.Blocked`

        """
        validators.require_callback(callback)
        self.callbacks.add(0,
                           spec.Connection.Blocked,
                           functools.partial(callback, self),
                           one_shot=False)

    def add_on_connection_unblocked_callback(
        self, callback: Callable[
            [Connection, frame.Method[spec.Connection.Unblocked]], Any]
    ) -> None:
        """RabbitMQ AMQP extension - Add a callback to be notified when the
        connection gets unblocked (`Connection.Unblocked` frame is received from
        RabbitMQ) letting publishers know it's ok to start publishing again.

        :param callback: Callback to call on
            `Connection.Unblocked`, having the signature
            `callback(connection, pika.frame.Method)`, where the method frame's
            `method` member is of type `pika.spec.Connection.Unblocked`

        """
        validators.require_callback(callback)
        self.callbacks.add(0,
                           spec.Connection.Unblocked,
                           functools.partial(callback, self),
                           one_shot=False)

    def add_on_open_callback(self, callback: Callable[[Connection],
                                                      Any]) -> None:
        """
        Add a callback notification when the connection has opened.

        The callback will be passed the connection instance as its only arg.

        :param callback: Callback to call when open
        """
        validators.require_callback(callback)
        self.callbacks.add(0, self.ON_CONNECTION_OPEN_OK, callback, False)

    def add_on_open_error_callback(self,
                                   callback: Callable[
                                       [Connection, BaseException | None], Any],
                                   remove_default: bool = True) -> None:
        """
        Add a callback notification when the connection can not be opened.

        The callback method should accept the connection instance that could not connect, and either
        a string or an exception as its second arg.

        :param callback: Callback to call when can't connect, having the signature _(Connection,
            Exception)
        :param remove_default: Remove default exception raising callback
        """
        validators.require_callback(callback)
        if remove_default:
            self.callbacks.remove(0, self.ON_CONNECTION_ERROR,
                                  self._default_on_connection_error)
        self.callbacks.add(0, self.ON_CONNECTION_ERROR, callback, False)

    def remove_on_close_callback(
            self, callback: Callable[[Connection, Exception], Any]) -> bool:
        """
        Remove a callback registered via :meth:`add_on_close_callback`.

        :param callback: The callback to remove
        :returns: True if a callback was removed, otherwise False
        """
        return self.callbacks.remove_matching(0, self.ON_CONNECTION_CLOSED,
                                              lambda cb: cb == callback)

    def remove_on_connection_blocked_callback(
        self,
        callback: Callable[[Connection, frame.Method[spec.Connection.Blocked]],
                           Any]
    ) -> bool:
        """Remove a callback registered via
        :meth:`add_on_connection_blocked_callback`.

        The callback was wrapped in a ``functools.partial`` at registration time,
        so it is matched here by the original callable rather than by equality.

        :param callback: The callback to remove
        :returns: True if a callback was removed, otherwise False
        """

        def matches(stored: Callable[..., Any]) -> bool:
            return (isinstance(stored, functools.partial) and
                    stored.func == callback and stored.args == (self,))

        return self.callbacks.remove_matching(0, spec.Connection.Blocked,
                                              matches)

    def remove_on_connection_unblocked_callback(
        self, callback: Callable[
            [Connection, frame.Method[spec.Connection.Unblocked]], Any]
    ) -> bool:
        """Remove a callback registered via
        :meth:`add_on_connection_unblocked_callback`.

        The callback was wrapped in a ``functools.partial`` at registration time,
        so it is matched here by the original callable rather than by equality.

        :param callback: The callback to remove
        :returns: True if a callback was removed, otherwise False
        """

        def matches(stored: Callable[..., Any]) -> bool:
            return (isinstance(stored, functools.partial) and
                    stored.func == callback and stored.args == (self,))

        return self.callbacks.remove_matching(0, spec.Connection.Unblocked,
                                              matches)

    def remove_on_open_callback(self, callback: Callable[[Connection],
                                                         Any]) -> bool:
        """
        Remove a callback registered via :meth:`add_on_open_callback`.

        :param callback: The callback to remove
        :returns: True if a callback was removed, otherwise False
        """
        return self.callbacks.remove_matching(0, self.ON_CONNECTION_OPEN_OK,
                                              lambda cb: cb == callback)

    def remove_on_open_error_callback(
            self, callback: Callable[[Connection, BaseException | None],
                                     Any]) -> bool:
        """
        Remove a callback registered via :meth:`add_on_open_error_callback`.

        :param callback: The callback to remove
        :returns: True if a callback was removed, otherwise False
        """
        return self.callbacks.remove_matching(0, self.ON_CONNECTION_ERROR,
                                              lambda cb: cb == callback)

    def channel(
            self,
            channel_number: int | None = None,
            on_open_callback: Callable[[Channel], Any] | None = None
    ) -> Channel:
        """
        Create a new channel with the next available channel number or pass in a channel number to
        use. Must be non-zero if you would like to specify but it is recommended that you let Pika
        manage the channel numbers.

        :param channel_number: The channel number to use, defaults to the next available.
        :param on_open_callback: The callback when the channel is opened. The callback will be
            invoked with the `Channel` instance as its only argument.
        """
        if not self.is_open:
            raise exceptions.ConnectionWrongStateError(
                f'Channel allocation requires an open connection: {self}')

        validators.rpc_completion_callback(on_open_callback)

        if not channel_number:
            channel_number = self._next_channel_number()

        self._channels[channel_number] = self._create_channel(
            channel_number, on_open_callback)
        self._add_channel_callbacks(channel_number)
        self._channels[channel_number].open()
        return self._channels[channel_number]

    def update_secret(
        self,
        new_secret: str,
        reason: str,
        callback: Callable[[frame.Method[spec.Connection.UpdateSecretOk]],
                           Any] | None = None
    ) -> None:
        """RabbitMQ AMQP extension - This method updates the secret used to authenticate this connection.
        It is used when secrets have an expiration date and need to be renewed, like OAuth 2 tokens.
        Pass a callback to be notified of the response from the server.

        :param new_secret: The new secret
        :param reason: The reason for the secret update
        :param callback: Callback to call on
            `Connection.UpdateSecretOk`, having the signature
            `callback(pika.frame.Method)`, where the method frame's
            `method` member is of type `pika.spec.Connection.UpdateSecretOk`

        :raises pika.exceptions.ConnectionWrongStateError: if connection is
            not open.
        """
        if not self.is_open:
            raise exceptions.ConnectionWrongStateError(
                f'Secret update requires an open connection: {self}')

        validators.rpc_completion_callback(callback)
        self._rpc(0, spec.Connection.UpdateSecret(new_secret, reason), callback,
                  [spec.Connection.UpdateSecretOk])

    def close(self,
              reply_code: int = 200,
              reply_text: str = 'Normal shutdown') -> None:
        """
        Disconnect from RabbitMQ.

        If there are any open channels, it will attempt to close them prior to fully disconnecting.
        Channels which have active consumers will attempt to send a Basic.Cancel to RabbitMQ to
        cleanly stop the delivery of messages prior to closing the channel.

        :param reply_code: The code number for the close
        :param reply_text: The text reason for the close
        :raises pika.exceptions.ConnectionWrongStateError: if connection is closed or closing.
        """
        if self.is_closing or self.is_closed:
            msg = (
                f'Illegal close({reply_code}, {reply_text!r}) request on {self} because it '
                f'was called while connection state={self._STATE_NAMES[self.connection_state]}.'
            )
            LOGGER.error(msg)
            raise exceptions.ConnectionWrongStateError(msg)

        # NOTE The connection is either in opening or open state

        # Initiate graceful closing of channels that are OPEN or OPENING
        if self._channels:
            self._close_channels(reply_code, reply_text)

        prev_state = self.connection_state

        # Transition to closing
        self._set_connection_state(self.CONNECTION_CLOSING)
        LOGGER.info('Closing connection (%s): %r', reply_code, reply_text)

        if not self._opened:
            # It was opening, but not fully open yet, so we won't attempt
            # graceful AMQP Connection.Close.
            LOGGER.info('Connection.close() is terminating stream and '
                        'bypassing graceful AMQP close, since AMQP is still '
                        'opening.')

            error = exceptions.ConnectionOpenAborted(
                'Connection.close() called before connection '
                f'finished opening: prev_state={self._STATE_NAMES[prev_state]} ({reply_code}): {reply_text!r}',
                host=self.params.host,
                port=self.params.port)
            self._terminate_stream(error)

        else:
            self._error = exceptions.ConnectionClosedByClient(
                reply_code,
                reply_text,
                host=self.params.host,
                port=self.params.port)

            # If there are channels that haven't finished closing yet, then
            # _on_close_ready will finally be called from _on_channel_cleanup once
            # all channels have been closed
            if not self._channels:
                # We can initiate graceful closing of the connection right away,
                # since no more channels remain
                self._on_close_ready()
            else:
                LOGGER.info(
                    'Connection.close is waiting for %d channels to close: %s',
                    len(self._channels), self)

    #
    # Connection state properties
    #

    @property
    def is_closed(self) -> bool:
        """Returns a boolean reporting the current connection state."""
        return self.connection_state == self.CONNECTION_CLOSED

    @property
    def is_closing(self) -> bool:
        """Returns True if connection is in the process of closing due to client-initiated `close`
        request, but closing is not yet complete.
        """
        return self.connection_state == self.CONNECTION_CLOSING

    @property
    def is_open(self) -> bool:
        """Returns a boolean reporting the current connection state."""
        return self.connection_state == self.CONNECTION_OPEN

    #
    # Properties that reflect server capabilities for the current connection
    #

    @property
    def basic_nack(self) -> bool:
        """Specifies if the server supports basic.nack on the active connection."""
        if self.server_capabilities is None:
            return False
        return self.server_capabilities.get('basic.nack', False)

    @property
    def consumer_cancel_notify(self) -> bool:
        """Specifies if the server supports consumer cancel notification on the active
        connection.
        """
        if self.server_capabilities is None:
            return False
        return self.server_capabilities.get('consumer_cancel_notify', False)

    @property
    def exchange_exchange_bindings(self) -> bool:
        """Specifies if the active connection supports exchange to exchange bindings."""
        if self.server_capabilities is None:
            return False
        return self.server_capabilities.get('exchange_exchange_bindings', False)

    @property
    def publisher_confirms(self) -> bool:
        """Specifies if the active connection can use publisher confirmations."""
        if self.server_capabilities is None:
            return False
        return self.server_capabilities.get('publisher_confirms', False)

    @abc.abstractmethod
    def _adapter_call_later(self, delay: float,
                            callback: Callable[[], Any]) -> object:
        """
        Adapters should override to call the callback after the specified number of seconds have
        elapsed, using a timer, or a thread, or similar.

        :param delay: The number of seconds to wait to call callback
        :param callback: The callback will be called without args.
        :returns: Handle that can be passed to `_adapter_remove_timeout()` to cancel the callback.
        """

    @abc.abstractmethod
    def _adapter_remove_timeout(self, timeout_id: object) -> None:
        """
        Adapters should override: Remove a timeout.

        :param timeout_id: The timeout handle to remove
        """

    @abc.abstractmethod
    def _adapter_add_callback_threadsafe(self, callback: Callable[...,
                                                                  Any]) -> None:
        """
        Requests a call to the given function as soon as possible in the context of this
        connection's IOLoop thread.

        NOTE: This is the only thread-safe method offered by the connection. All
         other manipulations of the connection must be performed from the
         connection's thread.

        :param callback: The callback method; must be callable.
        """

    #
    # Internal methods for managing the communication process
    #
    @abc.abstractmethod
    def _adapter_connect_stream(self) -> None:
        """
        Subclasses should override to initiate stream connection workflow asynchronously.

        Upon failed or aborted completion, they must invoke `Connection._on_stream_terminated()`.

        NOTE: On success, the stack will be up already, so there is no
              corresponding callback.
        """

    @abc.abstractmethod
    def _adapter_disconnect_stream(self) -> None:
        """Asynchronously bring down the streaming transport layer and invoke
        `Connection._on_stream_terminated()` asynchronously when complete.
        """

    @abc.abstractmethod
    def _adapter_emit_data(self, data: bytes) -> None:
        """
        Take ownership of data and send it to AMQP server as soon as possible.

        Subclasses must override this

        :param data:
        """

    def _add_channel_callbacks(self, channel_number: int) -> None:
        """
        Add the appropriate callbacks for the specified channel number.

        :param channel_number: The channel number for the callbacks
        """
        # This permits us to garbage-collect our reference to the channel
        # regardless of whether it was closed by client or broker, and do so
        # after all channel-close callbacks.
        self._channels[channel_number]._add_on_cleanup_callback(
            self._on_channel_cleanup)

    def _add_connection_start_callback(self) -> None:
        """Add a callback for when a Connection.Start frame is received from the broker."""
        self.callbacks.add(0, spec.Connection.Start, self._on_connection_start)

    def _add_connection_tune_callback(self) -> None:
        """Add a callback for when a Connection.Tune frame is received."""
        self.callbacks.add(0, spec.Connection.Tune, self._on_connection_tune)

    def _check_for_protocol_mismatch(
            self, value: frame.Method[spec.Connection.Start]) -> None:
        """
        Invoked when starting a connection to make sure it's a supported protocol.

        :param value: The frame to check
        :raises ProtocolVersionMismatch:
        """
        if ((value.method.version_major, value.method.version_minor)
                != spec.PROTOCOL_VERSION[0:2]):
            raise exceptions.ProtocolVersionMismatch(frame.ProtocolHeader(),
                                                     value)

    @property
    def _client_properties(self) -> dict[str, Any]:
        """Return the client properties dictionary."""
        properties = {
            'product': PRODUCT,
            'platform': f'Python {platform.python_version()}',
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'exchange_exchange_bindings': True,
                'publisher_confirms': True
            },
            'information': 'See https://pika.rtfd.org',
            'version': pika.__version__
        }

        if self.params.client_properties:
            properties.update(self.params.client_properties)

        return properties

    def _close_channels(self, reply_code: int, reply_text: str) -> None:
        """
        Initiate graceful closing of channels that are in OPEN or OPENING states, passing reply_code
        and reply_text.

        :param reply_code: The code for why the channels are being closed
        :param reply_text: The text reason for why the channels are closing
        """
        assert self.is_open, str(self)

        for channel_number in list(self._channels.keys()):
            chan = self._channels[channel_number]
            if not (chan.is_closing or chan.is_closed):
                chan.close(reply_code, reply_text)

    def _create_channel(
            self, channel_number: int,
            on_open_callback: Callable[[Channel], Any] | None) -> Channel:
        """
        Create a new channel using the specified channel number and calling back the method
        specified by on_open_callback.

        :param channel_number: The channel number to use
        :param on_open_callback: The callback when the channel is opened. The callback will be
            invoked with the `Channel` instance as its only argument.
        """
        LOGGER.debug('Creating channel %s', channel_number)
        return pika.channel.Channel(self, channel_number, on_open_callback)

    def _create_heartbeat_checker(
            self) -> pika.heartbeat.HeartbeatChecker | None:
        """Create a heartbeat checker instance if there is a heartbeat interval set."""
        if isinstance(self.params.heartbeat, int) and self.params.heartbeat > 0:
            LOGGER.debug('Creating a HeartbeatChecker: %r',
                         self.params.heartbeat)
            return pika.heartbeat.HeartbeatChecker(self, self.params.heartbeat)

        return None

    def _remove_heartbeat(self) -> None:
        """Stop the heartbeat checker if it exists."""
        if self._heartbeat_checker:
            self._heartbeat_checker.stop()
            self._heartbeat_checker = None

    def _deliver_frame_to_channel(self, value: frame.Frame) -> None:
        """
        Deliver the frame to the channel specified in the frame.

        :param value: The frame to deliver
        """
        if value.channel_number not in self._channels:
            # This should never happen and would constitute breach of the
            # protocol
            LOGGER.critical(
                'Received %s frame for unregistered channel %i on %s',
                value.NAME, value.channel_number, self)
            return

        self._channels[value.channel_number]._handle_content_frame(value)

    def _ensure_closed(self) -> None:
        """If the connection is not closed, close it."""
        if self.is_open:
            self.close()

    def _get_body_frame_max_length(self) -> int:
        """Calculate the maximum amount of bytes that can be in a body frame."""
        return (self.params.frame_max - spec.FRAME_HEADER_SIZE -
                spec.FRAME_END_SIZE)

    def _get_credentials(
        self, method_frame: frame.Method[spec.Connection.Start]
    ) -> tuple[str, bytes | None]:
        """
        Get credentials for authentication.

        :param method_frame: The Connection.Start frame
        """
        (auth_type,
         response) = self.params.credentials.response_for(method_frame.method)
        if not auth_type:
            raise exceptions.AuthenticationError(self.params.credentials.TYPE,
                                                 host=self.params.host,
                                                 port=self.params.port)
        self.params.credentials.erase_credentials()
        return auth_type, response

    def _has_pending_callbacks(
            self, value: frame.Method[amqp_object.Method]) -> int | None:
        """
        Return true if there are any callbacks pending for the specified frame.

        :param value: The frame to check
        """
        return self.callbacks.pending(value.channel_number, value.method)

    def _is_method_frame(self, value: frame.Frame) -> bool:
        """
        Returns true if the frame is a method frame.

        :param value: The frame to evaluate
        """
        return value.frame_type == spec.FRAME_METHOD

    def _is_protocol_header_frame(self, value: frame.Frame) -> bool:
        """
        Returns True if it's a protocol header frame.

        :param value: Frame to inspect
        """
        return isinstance(value, frame.ProtocolHeader)

    def _next_channel_number(self) -> int:
        """Return the next available channel number or raise an exception."""
        limit = self.params.channel_max or pika.channel.MAX_CHANNELS
        if len(self._channels) >= limit:
            raise exceptions.NoFreeChannels(host=self.params.host,
                                            port=self.params.port)

        for num in range(1, len(self._channels) + 1):
            if num not in self._channels:
                return num
        return len(self._channels) + 1

    def _on_channel_cleanup(self, channel: Channel) -> None:
        """
        Remove the channel from the dict of channels when Channel.CloseOk is sent.

        If connection is closing and no more channels remain, proceed to `_on_close_ready`.

        :param channel: channel instance
        """
        try:
            del self._channels[channel.channel_number]
            LOGGER.debug('Removed channel %s', channel.channel_number)
        except KeyError:
            LOGGER.error('Channel %r not in channels', channel.channel_number)
        if self.is_closing:
            if not self._channels:
                # Initiate graceful closing of the connection
                self._on_close_ready()
            else:
                # Once Connection enters CLOSING state, all remaining channels
                # should also be in CLOSING state. Deviation from this would
                # prevent Connection from completing its closing procedure.
                channels_not_in_closing_state = [
                    chan for chan in self._channels.values()
                    if not chan.is_closing
                ]
                if channels_not_in_closing_state:
                    LOGGER.critical(
                        'Connection in CLOSING state has non-CLOSING '
                        'channels: %r', channels_not_in_closing_state)

    def _on_close_ready(self) -> None:
        """
        Called when the Connection is in a state that it can close after a close has been requested
        by client.

        This happens after all of the channels are closed that were open when the close request was
        made.
        """
        if self.is_closed:
            LOGGER.warning('_on_close_ready invoked when already closed')
            return

        assert isinstance(self._error, exceptions.ConnectionClosed)
        self._send_connection_close(self._error.reply_code,
                                    self._error.reply_text)

    def _on_stream_connected(self) -> None:
        """Invoked when the socket is connected and it's time to start speaking AMQP with the
        broker.
        """
        self._set_connection_state(self.CONNECTION_PROTOCOL)

        # Start the communication with the RabbitMQ Broker
        self._send_frame(frame.ProtocolHeader())

    def _on_blocked_connection_timeout(self) -> None:
        """
        Called when the "connection blocked timeout" expires.

        When this happens, we tear down the connection
        """
        self._blocked_conn_timer = None
        self._terminate_stream(
            exceptions.ConnectionBlockedTimeout(
                'Blocked connection timeout expired.',
                host=self.params.host,
                port=self.params.port))

    def _on_connection_blocked(
            self, _connection: Connection,
            method_frame: frame.Method[spec.Connection.Blocked]) -> None:
        """
        Handle Connection.Blocked notification from RabbitMQ broker.

        :param _connection: The connection instance (unused)
        :param method_frame: method frame having `method` member of type
            `pika.spec.Connection.Blocked`
        """
        LOGGER.warning('Received %s from broker', method_frame)

        if self._blocked_conn_timer is not None:
            # RabbitMQ is not supposed to repeat Connection.Blocked, but it
            # doesn't hurt to be careful
            LOGGER.warning(
                '_blocked_conn_timer %s already set when '
                '_on_connection_blocked is called', self._blocked_conn_timer)
        else:
            assert self.params.blocked_connection_timeout is not None
            self._blocked_conn_timer = self._adapter_call_later(
                self.params.blocked_connection_timeout,
                self._on_blocked_connection_timeout)

    def _on_connection_unblocked(
            self, _connection: Connection,
            method_frame: frame.Method[spec.Connection.Unblocked]) -> None:
        """
        Handle Connection.Unblocked notification from RabbitMQ broker.

        :param _connection: The connection instance (unused)
        :param method_frame: method frame having `method` member of type
            `pika.spec.Connection.Blocked`
        """
        LOGGER.info('Received %s from broker', method_frame)

        if self._blocked_conn_timer is None:
            # RabbitMQ is supposed to pair Connection.Blocked/Unblocked, but it
            # doesn't hurt to be careful
            LOGGER.warning('_blocked_conn_timer was not active when '
                           '_on_connection_unblocked called')
        else:
            self._adapter_remove_timeout(self._blocked_conn_timer)
            self._blocked_conn_timer = None

    def _on_connection_close_from_broker(
            self, method_frame: frame.Method[spec.Connection.Close]) -> None:
        """
        Called when the connection is closed remotely via Connection.Close frame from broker.

        :param method_frame: The Connection.Close frame
        """
        LOGGER.debug('_on_connection_close_from_broker: frame=%s', method_frame)

        # The generated spec types reply_code as optional, but a frame
        # decoded off the wire always carries it.
        reply_code = method_frame.method.reply_code
        assert reply_code is not None

        self._terminate_stream(
            exceptions.ConnectionClosedByBroker(reply_code,
                                                method_frame.method.reply_text,
                                                host=self.params.host,
                                                port=self.params.port))

    def _on_connection_close_ok(
            self, method_frame: frame.Method[spec.Connection.CloseOk]) -> None:
        """
        Called when Connection.CloseOk is received from remote.

        :param method_frame: The Connection.CloseOk frame
        """
        LOGGER.debug('_on_connection_close_ok: frame=%s', method_frame)

        self._terminate_stream(None)

    def _default_on_connection_error(self, _connection_unused: Connection,
                                     error: Exception) -> None:
        """
        Default behavior when the connecting connection cannot connect and user didn't supply own
        `on_connection_error` callback.

        :param _connection_unused: The connection instance (unused)
        :param error: The exception that caused the failure
        :raises Exception: the given error
        """
        raise error

    def _on_connection_open_ok(
            self, method_frame: frame.Method[spec.Connection.OpenOk]) -> None:
        """
        This is called once we have tuned the connection with the server and called the
        Connection.Open on the server and it has replied with Connection.Ok.

        :param method_frame: Server response frame
        """
        self._opened = True

        self.known_hosts = method_frame.method.known_hosts

        # We're now connected at the AMQP level
        self._set_connection_state(self.CONNECTION_OPEN)

        # Call our initial callback that we're open
        self.callbacks.process(0, self.ON_CONNECTION_OPEN_OK, self, self)

    def _on_connection_start(
            self, method_frame: frame.Method[spec.Connection.Start]) -> None:
        """
        This is called as a callback once we have received a Connection.Start from the server.

        :param method_frame: The frame received
        :raises UnexpectedFrameError:
        """
        self._set_connection_state(self.CONNECTION_START)

        try:
            if self._is_protocol_header_frame(method_frame):
                raise exceptions.UnexpectedFrameError(method_frame)
            self._check_for_protocol_mismatch(method_frame)
            self._set_server_information(method_frame)
            self._add_connection_tune_callback()
            self._send_connection_start_ok(*self._get_credentials(method_frame))
        except Exception as error:
            LOGGER.exception('Error processing Connection.Start.')
            self._terminate_stream(error)

    @staticmethod
    def _negotiate_integer_value(client_value: int | None,
                                 server_value: int | None) -> int:
        """
        Negotiates two values.

        If either of them is 0 or None, returns the other one. If both are positive integers,
        returns the smallest one.

        :param client_value: The client value
        :param server_value: The server value
        """
        if client_value is None:
            client_value = 0
        if server_value is None:
            server_value = 0

        # this is consistent with how Java client and Bunny
        # perform negotiation, see pika/pika#874
        if client_value == 0 or server_value == 0:
            val = max(client_value, server_value)
        else:
            val = min(client_value, server_value)

        return val

    @staticmethod
    def _tune_heartbeat_timeout(client_value: int | None,
                                server_value: int) -> int:
        """
        Determine heartbeat timeout per AMQP 0-9-1 rules.

        Per https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf,

        > Both peers negotiate the limits to the lowest agreed value as follows:
        > - The server MUST tell the client what limits it proposes.
        > - The client responds and **MAY reduce those limits** for its
            connection

        If the client specifies a value, it always takes precedence.

        :param client_value: None to accept server_value; otherwise, an integral
            number in seconds; 0 (zero) to disable heartbeat.
        :param server_value: integral value of the heartbeat timeout proposed by
            broker; 0 (zero) to disable heartbeat.

        :returns: the value of the heartbeat timeout to use and return to broker
        """
        if client_value is None:
            # Accept server's limit
            timeout = server_value
        else:
            timeout = client_value

        return timeout

    def _on_connection_tune(
            self, method_frame: frame.Method[spec.Connection.Tune]) -> None:
        """
        Once the Broker sends back a Connection.Tune, we will set our tuning variables that have
        been returned to us and kick off the Heartbeat monitor if required, send our TuneOk and then
        the Connection. Open rpc call on channel 0.

        :param method_frame: The frame received
        """
        self._set_connection_state(self.CONNECTION_TUNE)

        # Get our max channels, frames and heartbeat interval
        self.params.channel_max = Connection._negotiate_integer_value(
            self.params.channel_max, method_frame.method.channel_max)
        self.params.frame_max = Connection._negotiate_integer_value(
            self.params.frame_max, method_frame.method.frame_max)

        if callable(self.params.heartbeat):
            ret_heartbeat = self.params.heartbeat(self,
                                                  method_frame.method.heartbeat)
            if ret_heartbeat is None or callable(ret_heartbeat):
                # Enforce callback-specific restrictions on callback's return value
                raise TypeError('heartbeat callback must not return None '
                                f'or callable, but got {ret_heartbeat!r}')

            # Leave it to heartbeat setter deal with the rest of the validation
            self.params.heartbeat = ret_heartbeat

        # Negotiate heartbeat timeout
        self.params.heartbeat = self._tune_heartbeat_timeout(
            client_value=self.params.heartbeat,
            server_value=method_frame.method.heartbeat)

        # Calculate the maximum pieces for body frames
        self._body_max_length = self._get_body_frame_max_length()

        # Create a new heartbeat checker if needed
        self._heartbeat_checker = self._create_heartbeat_checker()

        # Send the TuneOk response with what we've agreed upon
        self._send_connection_tune_ok()

        # Send the Connection.Open RPC call for the vhost
        self._send_connection_open()

    def _on_data_available(self, data_in: bytes) -> None:
        """
        This is called by our Adapter, passing in the data from the socket.

        As long as we have buffer try and map out frame data.

        :param data_in: The data that is available to read
        """
        buffer = self._frame_buffer
        buffer += data_in

        if self._processing_frame_buffer:
            # Re-entrant call: a frame callback caused more data to be read.
            # The data was appended above; the outer invocation's loop will
            # decode it, since trimming mid-loop would corrupt its offset.
            return

        # Decode frames by walking an offset and trim the buffer once per data
        # event; trimming (copying) the buffer after every frame is quadratic
        # when one read contains many frames.
        self._processing_frame_buffer = True
        offset = 0
        try:
            while offset < len(buffer):
                consumed_count, frame_value = frame.decode_frame(buffer, offset)
                if not frame_value:
                    break
                offset += consumed_count
                self.bytes_received += consumed_count
                self._process_frame(frame_value)
                if self._frame_buffer is not buffer:
                    # A frame callback reset the connection state (e.g. stream
                    # terminated); leftover data is no longer relevant.
                    return
        finally:
            self._processing_frame_buffer = False
            if offset and self._frame_buffer is buffer:
                del buffer[:offset]

    def _terminate_stream(self, error: Exception | None) -> None:
        """
        Deactivate heartbeat instance if activated already, and initiate termination of the stream
        (TCP) connection asynchronously.

        When connection terminates, the appropriate user callback will be
        invoked with the given error: "on open error" or "on connection closed".

        :param error: Exception (or None) describing the reason
            for termination; None for normal closing, such as upon receipt of
            Connection.CloseOk.
        """
        assert isinstance(error, (type(None), Exception)), (
            f'error arg is neither None nor instance of Exception: {error!r}.')

        if error is not None:
            # Save the exception for user callback once the stream closes
            self._error = error
        else:
            assert self._error is not None, (
                '_terminate_stream() expected self._error to be set when '
                'passed None error arg.')

        # So it won't mess with the stack
        self._remove_heartbeat()

        # Begin disconnection of stream or termination of connection workflow
        self._adapter_disconnect_stream()

    def _on_stream_terminated(self, error: Exception | None) -> None:
        """
        Handle termination of stack (including TCP layer) or failure to establish the stack.

        Notify registered ON_CONNECTION_ERROR or ON_CONNECTION_CLOSED callbacks, depending on
        whether the connection was opening or open.

        :param error: Exception (or None). None means that the transport was aborted internally and
            exception in `self._error` represents the cause. Otherwise it's an exception object that
            describes the unexpected loss of connection.
        """
        LOGGER.info(
            'AMQP stack terminated, failed to connect, or aborted: '
            'opened=%r, error-arg=%r; pending-error=%r', self._opened, error,
            self._error)

        if error is not None:
            if self._error is not None:
                LOGGER.debug(
                    '_on_stream_terminated(): overriding '
                    'pending-error=%r with %r', self._error, error)
            self._error = error
        else:
            assert self._error is not None, (
                '_on_stream_terminated() expected self._error to be populated '
                'with reason for terminating stack.')

        # Stop the heartbeat checker if it exists
        self._remove_heartbeat()

        # Remove connection management callbacks
        self._remove_callbacks(0,
                               [spec.Connection.Close, spec.Connection.Start])

        if self.params.blocked_connection_timeout is not None:
            self._remove_callbacks(
                0, [spec.Connection.Blocked, spec.Connection.Unblocked])

        if not self._opened and isinstance(
                self._error,
            (exceptions.StreamLostError, exceptions.ConnectionClosedByBroker)):
            # Heuristically deduce error based on connection state
            if self.connection_state == self.CONNECTION_PROTOCOL:
                LOGGER.error('Probably incompatible Protocol Versions')
                self._error = exceptions.IncompatibleProtocolError(
                    repr(self._error),
                    host=self.params.host,
                    port=self.params.port)
            elif self.connection_state == self.CONNECTION_START:
                LOGGER.error(
                    'Connection closed while authenticating indicating a '
                    'probable authentication error')
                self._error = exceptions.ProbableAuthenticationError(
                    repr(self._error),
                    host=self.params.host,
                    port=self.params.port)
            elif self.connection_state == self.CONNECTION_TUNE:
                LOGGER.error('Connection closed while tuning the connection '
                             'indicating a probable permission error when '
                             'accessing a virtual host')
                self._error = exceptions.ProbableAccessDeniedError(
                    repr(self._error),
                    host=self.params.host,
                    port=self.params.port)
            elif self.connection_state not in [
                    self.CONNECTION_OPEN, self.CONNECTION_CLOSED,
                    self.CONNECTION_CLOSING
            ]:
                LOGGER.warning('Unexpected connection state on disconnect: %i',
                               self.connection_state)

        # Transition to closed state
        self._set_connection_state(self.CONNECTION_CLOSED)

        # Inform our channel proxies, if any are still around
        for channel in list(self._channels.keys()):
            if channel not in self._channels:
                continue

            self._channels[channel]._on_close_meta(self._error)

        # Inform interested parties
        if not self._opened:
            LOGGER.info('Connection setup terminated due to %r', self._error)
            self.callbacks.process(0, self.ON_CONNECTION_ERROR, self, self,
                                   self._error)
        else:
            LOGGER.info('Stack terminated due to %r', self._error)
            self.callbacks.process(0, self.ON_CONNECTION_CLOSED, self, self,
                                   self._error)

        # Reset connection properties
        self._init_connection_state()

    def _process_callbacks(self, frame_value: frame.Frame) -> bool:
        """
        Process the callbacks for the frame if the frame is a method frame and if it has any
        callbacks pending.

        :param frame_value: The frame to process
        """
        if (self._is_method_frame(frame_value) and
                self._has_pending_callbacks(cast(frame.Method, frame_value))):
            self.callbacks.process(
                cast(frame.Method, frame_value).channel_number,  # Prefix
                cast(frame.Method, frame_value).method,  # Key
                self,  # Caller
                cast(frame.Method, frame_value))  # Args
            return True
        return False

    def _process_frame(
        self, frame_value: (frame.Frame | frame.ProtocolHeader)) -> None:
        """
        Process an inbound frame from the socket.

        :param frame_value: The frame to process
        """
        # Will receive a frame type of -1 if protocol version mismatch
        frame_type = frame_value.frame_type
        if frame_type < 0:
            return

        # ProtocolHeader (frame_type == -1) is handled above; narrow
        # the type for mypy.
        assert not isinstance(frame_value, frame.ProtocolHeader)

        # Keep track of how many frames have been read
        self.frames_received += 1

        # Process any callbacks, if True, exit method
        if self._process_callbacks(frame_value):
            return

        # If a heartbeat is received, update the checker
        if frame_type == spec.FRAME_HEARTBEAT:
            if self._heartbeat_checker:
                self._heartbeat_checker.received()
            else:
                LOGGER.warning('Received heartbeat frame without a heartbeat '
                               'checker')

        # If the frame has a channel number beyond the base channel, deliver it
        elif frame_value.channel_number > 0:
            self._deliver_frame_to_channel(frame_value)

    def _remove_callbacks(
            self, channel_number: int,
            method_classes: Sequence[type[amqp_object.Method]]) -> None:
        """
        Remove the callbacks for the specified channel number and list of method frames.

        :param channel_number: The channel number to remove the callback on
        :param method_classes: The method classes (derived from `pika.amqp_object.Method`) for the
            callbacks
        """
        for method_cls in method_classes:
            self.callbacks.remove(str(channel_number), method_cls)

    def _rpc(
        self,
        channel_number: int,
        method: amqp_object.Method,
        callback: Callable[..., Any] | None = None,
        acceptable_replies: Sequence[type[amqp_object.Method]] | None = None
    ) -> None:
        """
        Make an RPC call for the given callback, channel number and method.

        acceptable_replies lists out what responses we'll process from the server with the specified
        callback.

        :param channel_number: The channel number for the RPC call
        :param method: The method frame to call
        :param callback: The callback for the RPC response
        :param acceptable_replies: The replies this RPC call expects
        """
        # Validate that acceptable_replies is a list or None
        if acceptable_replies and not isinstance(acceptable_replies, list):
            raise TypeError('acceptable_replies should be list or None')

        # Validate the callback is callable
        if callback is not None:
            validators.require_callback(callback)
            assert acceptable_replies is not None
            for reply in acceptable_replies:
                self.callbacks.add(channel_number, reply, callback)

        # Send the rpc call to RabbitMQ
        self._send_method(channel_number, method)

    def _send_connection_close(self, reply_code: int, reply_text: str) -> None:
        """
        Send a Connection.Close method frame.

        :param reply_code: The reason for the close
        :param reply_text: The text reason for the close
        """
        self._rpc(0, spec.Connection.Close(reply_code, reply_text, 0, 0),
                  self._on_connection_close_ok, [spec.Connection.CloseOk])

    def _send_connection_open(self) -> None:
        """Send a Connection.Open frame."""
        self._rpc(0, spec.Connection.Open(self.params.virtual_host,
                                          insist=True),
                  self._on_connection_open_ok, [spec.Connection.OpenOk])

    def _send_connection_start_ok(self, authentication_type: str,
                                  response: bytes | None) -> None:
        """
        Send a Connection.StartOk frame.

        :param authentication_type: The auth type value
        :param response: The encoded value to send
        """
        self._send_method(
            0,
            spec.Connection.StartOk(self._client_properties,
                                    authentication_type, response,
                                    self.params.locale))

    def _send_connection_tune_ok(self) -> None:
        """Send a Connection.TuneOk frame."""
        # Tuning resolves any heartbeat callable and negotiates an integral
        # timeout before this is sent.
        heartbeat = self.params.heartbeat
        assert heartbeat is not None and not callable(heartbeat)

        self._send_method(
            0,
            spec.Connection.TuneOk(self.params.channel_max,
                                   self.params.frame_max, heartbeat))

    def _send_frame(
        self, frame_value: (frame.Frame | frame.ProtocolHeader)) -> None:
        """
        This appends the fully generated frame to send to the broker to the output buffer which will
        be then sent via the connection adapter.

        :param frame_value: The frame to write
        :raises exceptions.ConnectionClosed:
        """
        if self.is_closed:
            LOGGER.error('Attempted to send frame when closed')
            raise exceptions.ConnectionWrongStateError(
                'Attempted to send a frame on closed connection.')

        marshaled_frame = frame_value.marshal()
        self._output_marshaled_frames([marshaled_frame])

    def _send_method(
            self,
            channel_number: int,
            method: amqp_object.Method,
            content: tuple[spec.BasicProperties, bytes] | None = None) -> None:
        """
        Constructs a RPC method frame and then sends it to the broker.

        :param channel_number: The channel number for the frame
        :param method: The method to send
        :param content: If set, is a content frame, is tuple of properties and body.
        """
        if content:
            self._send_message(channel_number, method, content)
        else:
            self._send_frame(frame.Method(channel_number, method))

    def _send_message(self, channel_number: int,
                      method_frame: amqp_object.Method,
                      content: tuple[spec.BasicProperties, bytes]) -> None:
        """
        Publish a message.

        :param channel_number: The channel number for the frame
        :param method_frame: The method frame to send
        :param content: A content frame, which is tuple of properties and body.
        """
        length = len(content[1])
        marshaled_body_frames = []

        # Note: we construct the Method, Header and Content objects, marshal them
        # *then* output in case the marshaling operation throws an exception
        frame_method = frame.Method(channel_number, method_frame)
        frame_header = frame.Header(channel_number, length, content[0])
        marshaled_body_frames.append(frame_method.marshal())
        marshaled_body_frames.append(frame_header.marshal())

        if content[1]:
            chunks = math.ceil(float(length) / self._body_max_length)
            for chunk in range(chunks):
                start = chunk * self._body_max_length
                end = start + self._body_max_length
                end = min(end, length)
                frame_body = frame.Body(channel_number, content[1][start:end])
                marshaled_body_frames.append(frame_body.marshal())

        self._output_marshaled_frames(marshaled_body_frames)

    def _set_connection_state(self, connection_state: int) -> None:
        """
        Set the connection state.

        :param connection_state: The connection state to set
        """
        LOGGER.debug('New Connection state: %s (prev=%s)',
                     self._STATE_NAMES[connection_state],
                     self._STATE_NAMES[self.connection_state])

        self.connection_state = connection_state

    def _set_server_information(
            self, method_frame: frame.Method[spec.Connection.Start]) -> None:
        """
        Set the server properties and capabilities.

        :param method_frame: The Connection.Start frame
        """
        self.server_properties = method_frame.method.server_properties  # pyright: ignore[reportAttributeAccessIssue]
        assert self.server_properties is not None
        # Note: 'capabilities' is deliberately kept in server_properties.
        # Code that removed it never worked (it used hasattr on a dict, dead
        # since 2011), so callers have always seen the key there; removing it
        # now would break them.
        self.server_capabilities = self.server_properties.get(
            'capabilities', {})

    def _output_marshaled_frames(self,
                                 marshaled_frames: Sequence[bytes]) -> None:
        """
        Output list of marshaled frames to buffer and update stats.

        :param marshaled_frames: A list of frames marshaled to bytes
        """
        for marshaled_frame in marshaled_frames:
            self.bytes_sent += len(marshaled_frame)
            self.frames_sent += 1
            self._adapter_emit_data(marshaled_frame)
