"""
Connection parameter classes.

Defines the user-facing parameter objects (:class:`Parameters`, :class:`ConnectionParameters`,
:class:`URLParameters`) and :class:`SSLOptions`, extracted from :mod:`pika.connection` so that
module can host the public connection class.
"""

from __future__ import annotations

import ast
import copy
import logging
import numbers
import ssl
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import parse_qs as url_parse_qs
from urllib.parse import unquote as url_unquote
from urllib.parse import urlparse

import pika.channel
import pika.credentials
from pika import spec, validators
from pika._utils import override

if TYPE_CHECKING:
    from pika.connection import Connection

LOGGER = logging.getLogger(__name__)


class Parameters:
    """Base connection parameters class definition."""

    # Declare slots to protect against accidental assignment of an invalid
    # attribute
    __slots__ = (
        '_blocked_connection_timeout',
        '_channel_max',
        '_client_properties',
        '_connection_attempts',
        '_credentials',
        '_frame_max',
        '_heartbeat',
        '_host',
        '_locale',
        '_port',
        '_retry_delay',
        '_socket_timeout',
        '_ssl_options',
        '_stack_timeout',
        '_tcp_options',
        '_virtual_host',
    )

    DEFAULT_USERNAME = 'guest'
    DEFAULT_PASSWORD = 'guest'

    DEFAULT_BLOCKED_CONNECTION_TIMEOUT = None
    DEFAULT_CHANNEL_MAX = pika.channel.MAX_CHANNELS
    DEFAULT_CLIENT_PROPERTIES = None
    DEFAULT_CREDENTIALS = pika.credentials.PlainCredentials(
        DEFAULT_USERNAME, DEFAULT_PASSWORD)
    DEFAULT_CONNECTION_ATTEMPTS = 1
    DEFAULT_FRAME_MAX = spec.FRAME_MAX_SIZE
    DEFAULT_HEARTBEAT_TIMEOUT = None  # None accepts server's proposal
    DEFAULT_HOST = 'localhost'
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_PORT = 5672
    DEFAULT_RETRY_DELAY = 2.0
    DEFAULT_SOCKET_TIMEOUT = 10.0  # socket.connect() timeout
    DEFAULT_STACK_TIMEOUT = 15.0  # full-stack TCP/[SSl]/AMQP bring-up timeout
    DEFAULT_SSL = False
    DEFAULT_SSL_OPTIONS = None
    DEFAULT_SSL_PORT = 5671
    DEFAULT_VIRTUAL_HOST = '/'
    DEFAULT_TCP_OPTIONS = None

    def __init__(self) -> None:
        # If not None, blocked_connection_timeout is the timeout, in seconds,
        # for the connection to remain blocked; if the timeout expires, the
        # connection will be torn down, triggering the connection's
        # on_close_callback
        self._blocked_connection_timeout: float | None = None
        self._channel_max: int = 0
        self._client_properties: dict[str, Any] | None = None
        self._connection_attempts: int = 0
        self._credentials: (
            pika.credentials.PlainCredentials |
            pika.credentials.ExternalCredentials) = self.DEFAULT_CREDENTIALS
        self._frame_max: int = 0
        self._heartbeat: int | Callable[[Connection, float], int] | None = None
        self._host: str = ''
        self._locale: str = ''
        self._port: int = 0
        self._retry_delay: float = 0.0
        self._socket_timeout: float | None = None
        self._stack_timeout: float | None = None
        self._ssl_options: SSLOptions | None = None
        self._virtual_host: str = ''
        self._tcp_options: dict[str, int] | None = None

        self.blocked_connection_timeout = (
            self.DEFAULT_BLOCKED_CONNECTION_TIMEOUT)
        self.channel_max = self.DEFAULT_CHANNEL_MAX
        self.client_properties = self.DEFAULT_CLIENT_PROPERTIES
        self.connection_attempts = self.DEFAULT_CONNECTION_ATTEMPTS
        self.credentials = self.DEFAULT_CREDENTIALS
        self.frame_max = self.DEFAULT_FRAME_MAX
        self.heartbeat = self.DEFAULT_HEARTBEAT_TIMEOUT
        self.host = self.DEFAULT_HOST
        self.locale = self.DEFAULT_LOCALE
        self.port = self.DEFAULT_PORT
        self.retry_delay = self.DEFAULT_RETRY_DELAY
        self.socket_timeout = self.DEFAULT_SOCKET_TIMEOUT
        self.stack_timeout = self.DEFAULT_STACK_TIMEOUT
        self.ssl_options = self.DEFAULT_SSL_OPTIONS
        self.virtual_host = self.DEFAULT_VIRTUAL_HOST
        self.tcp_options = self.DEFAULT_TCP_OPTIONS

    @override
    def __repr__(self) -> str:
        """Represent the info about the instance."""
        return (
            f'<{self.__class__.__name__} host={self.host} port={self.port} virtual_host={self.virtual_host} ssl={bool(self.ssl_options)}>'
        )

    @override
    def __eq__(self, other) -> bool:
        if isinstance(other, Parameters):
            return self._host == other._host and self._port == other._port
        return NotImplemented

    @override
    def __ne__(self, other) -> bool:
        result = self.__eq__(other)
        if result is not NotImplemented:
            return not result
        return NotImplemented

    @property
    def blocked_connection_timeout(self) -> float | None:
        """
        :returns: blocked connection timeout. Defaults to
            `DEFAULT_BLOCKED_CONNECTION_TIMEOUT`.

        """
        return self._blocked_connection_timeout

    @blocked_connection_timeout.setter
    def blocked_connection_timeout(self, value: float | None) -> None:
        """
        :param value: If not None, blocked_connection_timeout is the timeout, in
            seconds, for the connection to remain blocked; if the timeout
            expires, the connection will be torn down, triggering the
            connection's on_close_callback

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('blocked_connection_timeout must be a Real '
                                f'number, but got {value!r}')
            if value < 0:
                raise ValueError('blocked_connection_timeout must be >= 0, but '
                                 f'got {value!r}')
        self._blocked_connection_timeout = value

    @property
    def channel_max(self) -> int:
        """
        :returns: max preferred number of channels. Defaults to
            `DEFAULT_CHANNEL_MAX`.

        """
        return self._channel_max

    @channel_max.setter
    def channel_max(self, value: int) -> None:
        """
        :param value: max preferred number of channels, between 1 and
           `channel.MAX_CHANNELS`, inclusive

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError(f'channel_max must be an int, but got {value!r}')
        if value < 1 or value > pika.channel.MAX_CHANNELS:
            raise ValueError(
                f'channel_max must be <= {pika.channel.MAX_CHANNELS} and > 0, but got {value!r}'
            )
        self._channel_max = value

    @property
    def client_properties(self) -> dict[str, Any] | None:
        """
        :returns: client properties used to override the fields in the default
            client properties reported  to RabbitMQ via `Connection.StartOk`
            method. Defaults to `DEFAULT_CLIENT_PROPERTIES`.

        """
        return self._client_properties

    @client_properties.setter
    def client_properties(self, value: dict[str, Any] | None) -> None:
        """
        :param value: None or dict of client properties used to override the
            fields in the default client properties reported to RabbitMQ via
            `Connection.StartOk` method.
        """
        if not isinstance(value, (
                dict,
                type(None),
        )):
            raise TypeError('client_properties must be dict or None, '
                            f'but got {value!r}')
        # Copy the mutable object to avoid accidental side-effects
        self._client_properties = copy.deepcopy(value)

    @property
    def connection_attempts(self) -> int:
        """
        :returns: number of socket connection attempts. Defaults to
            `DEFAULT_CONNECTION_ATTEMPTS`. See also `retry_delay`.

        """
        return self._connection_attempts

    @connection_attempts.setter
    def connection_attempts(self, value: int) -> None:
        """
        :param value: number of socket connection attempts of at least 1.
            See also `retry_delay`.

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('connection_attempts must be an int')
        if value < 1:
            raise ValueError(
                f'connection_attempts must be > 0, but got {value!r}')
        self._connection_attempts = value

    @property
    def credentials(
        self
    ) -> (pika.credentials.PlainCredentials |
          pika.credentials.ExternalCredentials):
        """
        One of the classes from `pika.credentials.VALID_TYPES`.

        Defaults to `DEFAULT_CREDENTIALS`.
        """
        return self._credentials

    @credentials.setter
    def credentials(
        self, value: (pika.credentials.PlainCredentials |
                      pika.credentials.ExternalCredentials)
    ) -> None:
        """
        :param value: authentication credential object of one of the classes
            from  `pika.credentials.VALID_TYPES`

        """
        if not isinstance(value, tuple(pika.credentials.VALID_TYPES)):
            raise TypeError(
                f'credentials must be an object of type: {pika.credentials.VALID_TYPES!r}, but '
                f'got {value!r}')
        # Copy the mutable object to avoid accidental side-effects
        self._credentials = copy.deepcopy(value)

    @property
    def frame_max(self) -> int:
        """
        :returns: desired maximum AMQP frame size to use. Defaults to
            `DEFAULT_FRAME_MAX`.

        """
        return self._frame_max

    @frame_max.setter
    def frame_max(self, value: int) -> None:
        """
        :param value: desired maximum AMQP frame size to use between
            `spec.FRAME_MIN_SIZE` and `spec.FRAME_MAX_SIZE`, inclusive

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError(f'frame_max must be an int, but got {value!r}')
        if value < spec.FRAME_MIN_SIZE:
            raise ValueError(
                f'Min AMQP 0.9.1 Frame Size is {spec.FRAME_MIN_SIZE}, but got {value!r}'
            )
        if value > spec.FRAME_MAX_SIZE:
            raise ValueError(
                f'Max AMQP 0.9.1 Frame Size is {spec.FRAME_MAX_SIZE}, but got {value!r}'
            )
        self._frame_max = value

    @property
    def heartbeat(self) -> int | Callable[[Connection, float], int] | None:
        """
        :returns: AMQP connection heartbeat timeout value for negotiation during
            connection tuning or callable which is invoked during connection tuning.
            None to accept broker's value. 0 turns heartbeat off. Defaults to
            `DEFAULT_HEARTBEAT_TIMEOUT`.

        """
        return self._heartbeat

    @heartbeat.setter
    def heartbeat(
            self,
            value: int | Callable[[Connection, float], int] | None) -> None:
        """
        :param value: Controls AMQP heartbeat timeout negotiation
            during connection tuning. An integer value always overrides the value
            proposed by broker. Use 0 to deactivate heartbeats and None to always
            accept the broker's proposal. If a callable is given, it will be called
            with the connection instance and the heartbeat timeout proposed by broker
            as its arguments. The callback should return a non-negative integer that
            will be used to override the broker's proposal.
        """
        if value is not None:
            if not isinstance(value, numbers.Integral) and not callable(value):
                raise TypeError(
                    f'heartbeat must be an int or a callable function, but got {value!r}'
                )
            if not callable(value) and value < 0:
                raise ValueError(f'heartbeat must >= 0, but got {value!r}')
        self._heartbeat = value  # pyright: ignore[reportAttributeAccessIssue]

    @property
    def host(self) -> str:
        """
        :returns: hostname or ip address of broker. Defaults to `DEFAULT_HOST`.

        """
        return self._host

    @host.setter
    def host(self, value: str) -> None:
        """
        :param value: hostname or ip address of broker

        """
        validators.require_string(value, 'host')
        self._host = value

    @property
    def locale(self) -> str:
        """
        :returns: locale value to pass to broker; e.g., 'en_US'. Defaults to
            `DEFAULT_LOCALE`.

        """
        return self._locale

    @locale.setter
    def locale(self, value: str) -> None:
        """
        :param value: locale value to pass to broker; e.g., "en_US"

        """
        validators.require_string(value, 'locale')
        self._locale = value

    @property
    def port(self) -> int:
        """
        :returns: port number of broker's listening socket. Defaults to
            `DEFAULT_PORT`.

        """
        return self._port

    @port.setter
    def port(self, value: int) -> None:
        """
        :param value: port number of broker's listening socket

        """
        try:
            self._port = int(value)
        except (TypeError, ValueError):
            raise TypeError(f'port must be an int, but got {value!r}')

    @property
    def retry_delay(self) -> float:
        """
        :returns: interval between socket connection attempts; see also
            `connection_attempts`. Defaults to `DEFAULT_RETRY_DELAY`.

        """
        return self._retry_delay

    @retry_delay.setter
    def retry_delay(self, value: float) -> None:
        """
        :param value: interval between socket connection attempts;
            see also `connection_attempts`.

        """
        if not isinstance(value, numbers.Real):
            raise TypeError(
                f'retry_delay must be a float or int, but got {value!r}')
        self._retry_delay = value

    @property
    def socket_timeout(self) -> float | None:
        """
        :returns: socket connect timeout in seconds. Defaults to
            `DEFAULT_SOCKET_TIMEOUT`. The value None disables this timeout.

        """
        return self._socket_timeout

    @socket_timeout.setter
    def socket_timeout(self, value: float | None) -> None:
        """
        :param value: positive socket connect timeout in
            seconds. None to disable this timeout.

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('socket_timeout must be a float or int, '
                                f'but got {value!r}')
            if value <= 0:
                raise ValueError(
                    f'socket_timeout must be > 0, but got {value!r}')
            value = float(value)

        self._socket_timeout = value

    @property
    def stack_timeout(self) -> float | None:
        """
        :returns: full protocol stack TCP/[SSL]/AMQP bring-up timeout in
            seconds. Defaults to `DEFAULT_STACK_TIMEOUT`. The value None
            disables this timeout.

        """
        return self._stack_timeout

    @stack_timeout.setter
    def stack_timeout(self, value: float | None) -> None:
        """
        :param value: positive full protocol stack
            TCP/[SSL]/AMQP bring-up timeout in seconds. It's recommended to set
            this value higher than `socket_timeout`. None to disable this
            timeout.

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('stack_timeout must be a float or int, '
                                f'but got {value!r}')
            if value <= 0:
                raise ValueError(
                    f'stack_timeout must be > 0, but got {value!r}')
            value = float(value)

        self._stack_timeout = value

    @property
    def ssl_options(self) -> SSLOptions | None:
        """
        :returns: None for plaintext or `pika.SSLOptions` instance for SSL/TLS.
        """
        return self._ssl_options

    @ssl_options.setter
    def ssl_options(self, value: SSLOptions | None) -> None:
        """
        :param value: None for plaintext or
            `pika.SSLOptions` instance for SSL/TLS. Defaults to None.

        """
        if not isinstance(value, (SSLOptions, type(None))):
            raise TypeError(
                f'ssl_options must be None or SSLOptions but got {value!r}')
        self._ssl_options = value

    @property
    def virtual_host(self) -> str:
        """
        :returns: rabbitmq virtual host name. Defaults to
            `DEFAULT_VIRTUAL_HOST`.

        """
        return self._virtual_host

    @virtual_host.setter
    def virtual_host(self, value: str) -> None:
        """
        :param value: rabbitmq virtual host name

        """
        validators.require_string(value, 'virtual_host')
        self._virtual_host = value

    @property
    def tcp_options(self) -> dict[str, int] | None:
        """
        :returns: None or a dict of options to pass to the underlying socket
        """
        return self._tcp_options

    @tcp_options.setter
    def tcp_options(self, value: dict[str, int] | None) -> None:
        """
        :param value: None or a dict of options to pass to the underlying
            socket. Currently supported are TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT
            and TCP_USER_TIMEOUT. Availability of these may depend on your platform.
        """
        if not isinstance(value, (dict, type(None))):
            raise TypeError(
                f'tcp_options must be a dict or None, but got {value!r}')
        self._tcp_options = value


class ConnectionParameters(Parameters):
    """Connection parameters object that is passed into the connection adapter upon construction."""

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ()

    class _DEFAULT:
        """Designates default parameter value; internal use."""

        ...  # noqa: PIE790, PYI013

    T = TypeVar('T')
    DefaultT = Union[T, Type[_DEFAULT]]

    def __init__(self,
                 host: DefaultT[str] = _DEFAULT,
                 port: DefaultT[int] = _DEFAULT,
                 virtual_host: DefaultT[str] = _DEFAULT,
                 credentials: DefaultT[(
                     pika.credentials.PlainCredentials |
                     pika.credentials.ExternalCredentials)] = _DEFAULT,
                 channel_max: DefaultT[int] = _DEFAULT,
                 frame_max: DefaultT[int] = _DEFAULT,
                 heartbeat: DefaultT[int | Callable[[Connection, float], int] |
                                     None] = _DEFAULT,
                 ssl_options: DefaultT[SSLOptions | None] = _DEFAULT,
                 connection_attempts: DefaultT[int] = _DEFAULT,
                 retry_delay: DefaultT[float] = _DEFAULT,
                 socket_timeout: DefaultT[float | None] = _DEFAULT,
                 stack_timeout: DefaultT[float | None] = _DEFAULT,
                 locale: DefaultT[str] = _DEFAULT,
                 blocked_connection_timeout: DefaultT[float | None] = _DEFAULT,
                 client_properties: DefaultT[dict[str, Any] | None] = _DEFAULT,
                 tcp_options: DefaultT[dict[str, Any] | None] = _DEFAULT,
                 **kwargs: Any) -> None:
        """
        Create a new ConnectionParameters instance.

        See `Parameters` for default values.

        :param host: Hostname or IP Address to connect to
        :param port: TCP port to connect to
        :param virtual_host: RabbitMQ virtual host to use
        :param credentials: auth credentials
        :param channel_max: Maximum number of channels to allow
        :param frame_max: The maximum byte size for an AMQP frame
        :param heartbeat: Controls AMQP heartbeat timeout negotiation
            during connection tuning. An integer value always overrides the value
            proposed by broker. Use 0 to deactivate heartbeats and None to always
            accept the broker's proposal. If a callable is given, it will be called
            with the connection instance and the heartbeat timeout proposed by broker
            as its arguments. The callback should return a non-negative integer that
            will be used to override the broker's proposal.
        :param ssl_options: None for plaintext or
            `pika.SSLOptions` instance for SSL/TLS. Defaults to None.
        :param connection_attempts: Maximum number of retry attempts
        :param retry_delay: Time to wait in seconds, before the next
        :param socket_timeout: Positive socket connect timeout in
            seconds.
        :param stack_timeout: Positive full protocol stack
            (TCP/[SSL]/AMQP) bring-up timeout in seconds. It's recommended to
            set this value higher than `socket_timeout`.
        :param locale: Set the locale value
        :param blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection:
            passing `ConnectionBlockedTimeout` exception to on_close_callback
            in asynchronous adapters or raising it in `BlockingConnection`.
        :param client_properties: None or dict of client properties used to
            override the fields in the default client properties reported to
            RabbitMQ via `Connection.StartOk` method.
        :param tcp_options: None or a dict of TCP options to set for socket
        """
        super().__init__()

        if blocked_connection_timeout is not self._DEFAULT:
            self.blocked_connection_timeout = cast(Optional[float],
                                                   blocked_connection_timeout)

        if channel_max is not self._DEFAULT:
            self.channel_max = cast(int, channel_max)

        if client_properties is not self._DEFAULT:
            self.client_properties = cast(Optional[Dict[str, Any]],
                                          client_properties)

        if connection_attempts is not self._DEFAULT:
            self.connection_attempts = cast(int, connection_attempts)

        if credentials is not self._DEFAULT:
            self.credentials = cast(
                Union[pika.credentials.PlainCredentials,
                      pika.credentials.ExternalCredentials], credentials)

        if frame_max is not self._DEFAULT:
            self.frame_max = cast(int, frame_max)

        if heartbeat is not self._DEFAULT:
            self.heartbeat = cast(
                'int | Callable[[Connection, float], int] | None', heartbeat)

        if host is not self._DEFAULT:
            self.host = cast(str, host)

        if locale is not self._DEFAULT:
            self.locale = cast(str, locale)

        if retry_delay is not self._DEFAULT:
            self.retry_delay = cast(float, retry_delay)

        if socket_timeout is not self._DEFAULT:
            self.socket_timeout = cast(Optional[float], socket_timeout)

        if stack_timeout is not self._DEFAULT:
            self.stack_timeout = cast(Optional[float], stack_timeout)

        if ssl_options is not self._DEFAULT:
            self.ssl_options = cast(Optional[SSLOptions], ssl_options)

        # Set port after SSL status is known
        if port is not self._DEFAULT:
            self.port = cast(int, port)
        else:
            self.port = self.DEFAULT_SSL_PORT if self.ssl_options else self.DEFAULT_PORT

        if virtual_host is not self._DEFAULT:
            self.virtual_host = cast(str, virtual_host)

        if tcp_options is not self._DEFAULT:
            self.tcp_options = cast(Optional[Dict[str, Any]], tcp_options)

        if kwargs:
            raise TypeError(f'unexpected kwargs: {kwargs!r}')


class URLParameters(Parameters):
    """Connect to RabbitMQ via an AMQP URL in the format::

         amqp://username:password@host:port/<virtual_host>[?query-string]

    Ensure that the virtual host is URI encoded when specified. For example if
    you are using the default "/" virtual host, the value should be `%2f`.

    See `Parameters` for default values.

    Valid query string values are:

        - channel_max:
            Override the default maximum channel count value
        - client_properties:
            dict of client properties used to override the fields in the default
            client properties reported to RabbitMQ via `Connection.StartOk`
            method
        - connection_attempts:
            Specify how many times pika should try and reconnect before it gives up
        - frame_max:
            Override the default maximum frame size for communication
        - heartbeat:
            Desired connection heartbeat timeout for negotiation. If not present
            the broker's value is accepted. 0 turns heartbeat off.
        - locale:
            Override the default `en_US` locale value
        - ssl_options:
            None for plaintext; for SSL: dict of public ssl context-related
            arguments that may be passed to :meth:`ssl.SSLSocket` as kwargs,
            except `sock`, `server_side`,`do_handshake_on_connect`, `family`,
            `type`, `proto`, `fileno`.
        - retry_delay:
            The number of seconds to sleep before attempting to connect on
            connection failure.
        - socket_timeout:
            Socket connect timeout value in seconds (float or int)
        - stack_timeout:
            Positive full protocol stack (TCP/[SSL]/AMQP) bring-up timeout in
            seconds. It's recommended to set this value higher than
            `socket_timeout`.
        - blocked_connection_timeout:
            Set the timeout, in seconds, that the connection may remain blocked
            (triggered by Connection.Blocked from broker); if the timeout
            expires before connection becomes unblocked, the connection will be
            torn down, triggering the connection's on_close_callback
        - tcp_options:
            Set the tcp options for the underlying socket.

    :param url: The AMQP URL to connect to

    """

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ('_all_url_query_values',)

    # The name of the private function for parsing and setting a given URL query
    # arg is constructed by catenating the query arg's name to this prefix
    _SETTER_PREFIX = '_set_url_'

    def __init__(self, url: str) -> None:
        """
        Create a new URLParameters instance.

        :param url: The URL value
        """
        super().__init__()

        self._all_url_query_values = None

        # Handle the Protocol scheme
        if url[0:4].lower() == 'amqp':
            url = 'http' + url[4:]

        parts = urlparse(url)

        if parts.scheme == 'https':
            # Create default context which will get overridden by the
            # ssl_options URL arg, if any
            self.ssl_options = pika.SSLOptions(
                context=ssl.create_default_context())
        elif parts.scheme == 'http':
            self.ssl_options = None
        elif parts.scheme:
            raise ValueError(
                f'Unexpected URL scheme {parts.scheme!r}; supported scheme '
                'values: amqp, amqps')

        if parts.hostname is not None:
            self.host = parts.hostname

        # Take care of port after SSL status is known
        if parts.port is not None:
            self.port = parts.port
        else:
            self.port = (self.DEFAULT_SSL_PORT
                         if self.ssl_options else self.DEFAULT_PORT)

        if parts.username is not None:
            assert parts.password is not None
            self.credentials = pika.credentials.PlainCredentials(
                url_unquote(parts.username), url_unquote(parts.password))

        # Get the Virtual Host
        if len(parts.path) > 1:
            self.virtual_host = url_unquote(parts.path.split('/')[1])

        # Handle query string values, validating and assigning them
        self._all_url_query_values = url_parse_qs(parts.query)

        for name, value in self._all_url_query_values.items():
            try:
                set_value = getattr(self, self._SETTER_PREFIX + name)
            except AttributeError:
                raise ValueError(f'Unknown URL parameter: {name!r}')

            try:
                (single_value,) = value
            except ValueError:
                raise ValueError(
                    f'Expected exactly one value for URL parameter '
                    f'{name}, but got {len(value)} values: {value}')

            set_value(single_value)

    def _set_url_blocked_connection_timeout(self, value: float) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            blocked_connection_timeout = float(value)
        except ValueError as exc:
            raise ValueError(
                f'Invalid blocked_connection_timeout value {value!r}: {exc!r}')
        self.blocked_connection_timeout = blocked_connection_timeout

    def _set_url_channel_max(self, value: int) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            channel_max = int(value)
        except ValueError as exc:
            raise ValueError(f'Invalid channel_max value {value!r}: {exc!r}')
        self.channel_max = channel_max

    def _set_url_client_properties(self, value: str) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        self.client_properties = ast.literal_eval(value)

    def _set_url_connection_attempts(self, value: int) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            connection_attempts = int(value)
        except ValueError as exc:
            raise ValueError(
                f'Invalid connection_attempts value {value!r}: {exc!r}')
        self.connection_attempts = connection_attempts

    def _set_url_frame_max(self, value: int) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            frame_max = int(value)
        except ValueError as exc:
            raise ValueError(f'Invalid frame_max value {value!r}: {exc!r}')
        self.frame_max = frame_max

    def _set_url_heartbeat(self, value: int) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            heartbeat_timeout = int(value)
        except ValueError as exc:
            raise ValueError(f'Invalid heartbeat value {value!r}: {exc!r}')
        self.heartbeat = heartbeat_timeout

    def _set_url_locale(self, value: str) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        self.locale = value

    def _set_url_retry_delay(self, value: float) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            retry_delay = float(value)
        except ValueError as exc:
            raise ValueError(f'Invalid retry_delay value {value!r}: {exc!r}')
        self.retry_delay = retry_delay

    def _set_url_socket_timeout(self, value: float) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            socket_timeout = float(value)
        except ValueError as exc:
            raise ValueError(f'Invalid socket_timeout value {value!r}: {exc!r}')
        self.socket_timeout = socket_timeout

    def _set_url_stack_timeout(self, value: float) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        try:
            stack_timeout = float(value)
        except ValueError as exc:
            raise ValueError(f'Invalid stack_timeout value {value!r}: {exc!r}')
        self.stack_timeout = stack_timeout

    def _set_url_ssl_options(self, value: str) -> None:
        """
        Deserialize and apply the corresponding query string arg.

        :param value: Raw query-string value to deserialize
        """
        opts = ast.literal_eval(value)
        if opts is None:
            if self.ssl_options is not None:
                raise ValueError(
                    'Specified ssl_options=None URI arg is inconsistent with '
                    'the specified amqps URI scheme.')
        else:
            # Older versions of Pika would take the opts dict and pass it
            # directly as kwargs to the deprecated ssl.wrap_socket method.
            # Here, we take the valid options and translate them into args
            # for various SSLContext methods.
            #
            # https://docs.python.org/3/library/ssl.html#ssl.wrap_socket
            #
            # SSLContext.load_verify_locations(cafile=None, capath=None, cadata=None)
            try:
                opt_protocol = ssl.PROTOCOL_TLS_CLIENT
            except AttributeError:
                opt_protocol = ssl.PROTOCOL_TLSv1_2
            if 'protocol' in opts:
                opt_protocol = opts['protocol']

            cxt = ssl.SSLContext(protocol=opt_protocol)

            opt_cafile = opts.get('ca_certs') or opts.get('cafile')
            opt_capath = opts.get('ca_path') or opts.get('capath')
            opt_cadata = opts.get('ca_data') or opts.get('cadata')
            cxt.load_verify_locations(opt_cafile, opt_capath, opt_cadata)

            # SSLContext.load_cert_chain(certfile, keyfile=None, password=None)
            if 'certfile' in opts:
                opt_certfile = opts['certfile']
                opt_keyfile = opts.get('keyfile')
                opt_password = opts.get('password')
                cxt.load_cert_chain(opt_certfile, opt_keyfile, opt_password)

            if 'ciphers' in opts:
                opt_ciphers = opts['ciphers']
                cxt.set_ciphers(opt_ciphers)

            server_hostname = opts.get('server_hostname')
            self.ssl_options = pika.SSLOptions(context=cxt,
                                               server_hostname=server_hostname)

    def _set_url_tcp_options(self, value: str) -> None:
        """Deserialize and apply the corresponding query string arg
        :param value: Raw query-string value to deserialize
        """
        self.tcp_options = ast.literal_eval(value)


class SSLOptions:
    """Class used to provide parameters for optional fine grained control of SSL socket wrapping."""

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ('context', 'server_hostname')

    def __init__(self,
                 context: ssl.SSLContext,
                 server_hostname: str | None = None) -> None:
        """
        :param context: SSLContext instance
        :param server_hostname: SSLContext.wrap_socket, used to enable
            SNI
        """
        if not isinstance(context, ssl.SSLContext):
            raise TypeError(
                f'context must be of ssl.SSLContext type, but got {context!r}')

        self.context = context
        self.server_hostname = server_hostname
