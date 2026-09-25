#!/usr/bin/env python3
"""A persistent-connection HTTP/1.1 calculator built on a bare TCP socket.

No framework and no http.server: just the socket module and the standard library.

    GET /add?a=2&b=3   -> 200  5
    GET /sub?a=10&b=4  -> 200  6
    GET /mul?a=6&b=7   -> 200  42
    GET /div?a=9&b=3   -> 200  3

The interesting part is not the arithmetic, it is *framing*. Because the
connection stays open, the server must know exactly where one request ends and
the next begins:

* the request head ends at the first CRLF CRLF;
* the body is then exactly Content-Length bytes, or a chunked body whose end is
  marked by a zero-size chunk and the trailer section;
* whatever is left over in the buffer is the start of the *next* request and is
  never thrown away.

Because every connection keeps its own byte buffer and requests are answered
one after another from that buffer, pipelined requests (several requests sent
before any response is read) are answered in order for free.

Usage:
    python server.py                    # listens on localhost:8080
    python server.py --port 9000 --idle-timeout 5
    python server.py --host 0.0.0.0     # expose on the network
"""
from __future__ import annotations

import argparse
import math
import operator
import re
import selectors
import socket
import sys
import threading
import time
from email.utils import formatdate
from urllib.parse import unquote, urlsplit

SERVER_NAME = "calc11/1.0"

# Limits. Each one exists so that a single client cannot make the server hold
# unbounded memory or a thread forever.
MAX_HEAD_BYTES = 8 * 1024        # request line + headers (and chunked trailers)
MAX_BODY_BYTES = 1024 * 1024     # a calculator has no use for large bodies
MAX_CHUNK_LINE = 1024            # "1a2b;ext=val" line in a chunked body
MAX_NUMBER_CHARS = 1000          # keeps big-integer arithmetic cheap
DEFAULT_IDLE_TIMEOUT = 10.0      # seconds a kept-alive connection may sit idle
DEFAULT_REQUEST_TIMEOUT = 30.0   # seconds to deliver one whole request once started
LINGER_SECONDS = 2.0             # how long to drain input before a final close

REASONS = {
    100: "Continue",
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    408: "Request Timeout",
    413: "Content Too Large",
    431: "Request Header Fields Too Large",
    500: "Internal Server Error",
    501: "Not Implemented",
    505: "HTTP Version Not Supported",
}

TOKEN_RE = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
VERSION_RE = re.compile(r"HTTP/(\d)\.(\d)")
CONTENT_LENGTH_RE = re.compile(r"\d{1,18}")
CHUNK_SIZE_RE = re.compile(rb"[0-9A-Fa-f]{1,16}")
INT_RE = re.compile(r"[+-]?\d+")
FLOAT_RE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class HTTPError(Exception):
    """A request the server answers with an error status.

    ``close`` is True when the error means we no longer know where the next
    request starts (bad framing), so the connection cannot safely be reused.
    """

    def __init__(self, status: int, message: str, close: bool = False):
        super().__init__(message)
        self.status = status
        self.message = message
        self.close = close


class PeerClosed(Exception):
    """The client closed its side of the connection."""


class ReadTimeout(Exception):
    """The client stopped sending before a request was complete."""


# --------------------------------------------------------------------------- #
# Buffered reading from the socket
# --------------------------------------------------------------------------- #
class Connection:
    """A socket plus the bytes read from it that nobody has consumed yet.

    Every read takes *exactly* what it asks for out of ``buf``; anything past
    that stays in the buffer for the next request. This is the whole trick of
    a persistent connection.
    """

    def __init__(self, sock: socket.socket, idle_timeout: float, request_timeout: float):
        self.sock = sock
        self.buf = bytearray()
        self.idle_timeout = idle_timeout
        self.request_timeout = request_timeout
        self.deadline: float | None = None

    def _fill(self, timeout: float) -> None:
        if timeout <= 0:
            raise ReadTimeout()
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(65536)
        except socket.timeout:
            raise ReadTimeout() from None
        if not data:
            raise PeerClosed()
        self.buf += data

    def fill(self) -> None:
        """Read more bytes while a request is in progress (bounded by its deadline)."""
        assert self.deadline is not None
        self._fill(min(self.idle_timeout, self.deadline - time.monotonic()))

    def wait_for_request(self) -> bool:
        """Block until the first byte of the next request arrives.

        Returns False when the connection should simply be closed: the client
        hung up, or it stayed idle for longer than the idle timeout.
        """
        while True:
            # RFC 9112 2.2: ignore empty lines received before a request-line.
            while self.buf[:2] == b"\r\n":
                del self.buf[:2]
            if self.buf and self.buf != b"\r":
                break
            try:
                self._fill(self.idle_timeout)
            except (PeerClosed, ReadTimeout):
                return False
        self.deadline = time.monotonic() + self.request_timeout
        return True

    def read_until(self, delimiter: bytes, limit: int, too_long: HTTPError) -> bytes:
        """Consume and return bytes up to ``delimiter`` (which is consumed too)."""
        start = 0
        while True:
            index = self.buf.find(delimiter, start)
            if index >= 0:
                if index > limit:
                    raise too_long
                data = bytes(self.buf[:index])
                del self.buf[: index + len(delimiter)]
                return data
            if len(self.buf) > limit:
                raise too_long
            start = max(0, len(self.buf) - len(delimiter) + 1)
            self.fill()

    def read_exact(self, n: int) -> bytes:
        """Consume and return exactly ``n`` bytes -- never n + 1."""
        while len(self.buf) < n:
            self.fill()
        data = bytes(self.buf[:n])
        del self.buf[:n]
        return data

    def send(self, data: bytes) -> None:
        self.sock.settimeout(self.request_timeout)
        self.sock.sendall(data)


# --------------------------------------------------------------------------- #
# Request parsing
# --------------------------------------------------------------------------- #
class Request:
    def __init__(self, method: str, target: str, version: tuple[int, int],
                 headers: list[tuple[str, str]], body: bytes):
        self.method = method
        self.target = target
        self.version = version
        self.headers = headers
        self.body = body

    def get_all(self, name: str) -> list[str]:
        return [value for key, value in self.headers if key == name]

    def tokens(self, name: str) -> set[str]:
        """Comma-separated tokens across all fields called ``name``, lower-cased."""
        return {part.strip().lower()
                for value in self.get_all(name)
                for part in value.split(",") if part.strip()}

    @property
    def keep_alive(self) -> bool:
        connection = self.tokens("connection")
        if "close" in connection:
            return False
        if self.version >= (1, 1):
            return True                      # HTTP/1.1: persistent by default
        return "keep-alive" in connection    # HTTP/1.0: only if asked for


def parse_head(head: bytes) -> tuple[str, str, tuple[int, int], list[tuple[str, str]]]:
    lines = head.split(b"\r\n")
    try:
        request_line = lines[0].decode("ascii")
    except UnicodeDecodeError:
        raise HTTPError(400, "request line is not ASCII", close=True) from None

    parts = request_line.split(" ")
    if len(parts) != 3 or not all(parts):
        raise HTTPError(400, "malformed request line", close=True)
    method, target, version_text = parts
    if not TOKEN_RE.fullmatch(method):
        raise HTTPError(400, "malformed method", close=True)
    match = VERSION_RE.fullmatch(version_text)
    if not match:
        raise HTTPError(400, "malformed HTTP version", close=True)
    version = (int(match.group(1)), int(match.group(2)))
    if version[0] != 1:
        raise HTTPError(505, "only HTTP/1.x is supported", close=True)

    headers = []
    for raw in lines[1:]:
        if raw[:1] in (b" ", b"\t"):
            raise HTTPError(400, "obsolete line folding is not allowed", close=True)
        name, colon, value = raw.partition(b":")
        if not colon:
            raise HTTPError(400, "header line without a colon", close=True)
        name_text = name.decode("latin-1")
        if not TOKEN_RE.fullmatch(name_text):
            # Also rejects "Name :" -- whitespace before the colon (RFC 9112 5.1).
            raise HTTPError(400, "malformed header name", close=True)
        if b"\r" in value or b"\n" in value or b"\x00" in value:
            raise HTTPError(400, "invalid character in header value", close=True)
        headers.append((name_text.lower(), value.decode("latin-1").strip(" \t")))
    return method, target, version, headers


def read_chunked_body(conn: Connection) -> bytes:
    """Decode a chunked body (RFC 9112 7.1) and consume its trailer section."""
    body = bytearray()
    while True:
        line = conn.read_until(b"\r\n", MAX_CHUNK_LINE,
                               HTTPError(400, "chunk size line too long", close=True))
        size_text = line.split(b";", 1)[0].strip(b" \t")
        if not CHUNK_SIZE_RE.fullmatch(size_text):
            raise HTTPError(400, "malformed chunk size", close=True)
        size = int(size_text, 16)
        if size == 0:
            break
        if len(body) + size > MAX_BODY_BYTES:
            raise HTTPError(413, "request body too large", close=True)
        body += conn.read_exact(size)
        if conn.read_exact(2) != b"\r\n":
            raise HTTPError(400, "chunk data not followed by CRLF", close=True)

    trailer_bytes = 0
    while True:
        line = conn.read_until(b"\r\n", MAX_HEAD_BYTES,
                               HTTPError(431, "trailer section too large", close=True))
        if not line:
            return bytes(body)
        trailer_bytes += len(line) + 2
        if trailer_bytes > MAX_HEAD_BYTES:
            raise HTTPError(431, "trailer section too large", close=True)


def read_body(conn: Connection, version: tuple[int, int],
              headers: list[tuple[str, str]]) -> bytes:
    """Work out how long the body is, then consume exactly that many bytes."""
    transfer_encoding = [v for k, v in headers if k == "transfer-encoding"]
    content_length = [v for k, v in headers if k == "content-length"]

    if transfer_encoding:
        # A message with both is a classic request-smuggling vector: refuse it.
        if content_length:
            raise HTTPError(400, "both Transfer-Encoding and Content-Length", close=True)
        if version < (1, 1):
            raise HTTPError(400, "Transfer-Encoding in an HTTP/1.0 request", close=True)
        codings = [c.strip().lower() for v in transfer_encoding for c in v.split(",") if c.strip()]
        if not codings or codings[-1] != "chunked":
            raise HTTPError(400, "chunked must be the final transfer coding", close=True)
        if codings != ["chunked"]:
            raise HTTPError(501, "unsupported transfer coding", close=True)
        return read_chunked_body(conn)

    if content_length:
        # "Content-Length: 5, 5" or two identical fields are tolerated; anything else is not.
        values = {part.strip() for v in content_length for part in v.split(",")}
        if len(values) != 1:
            raise HTTPError(400, "conflicting Content-Length values", close=True)
        value = values.pop()
        if not CONTENT_LENGTH_RE.fullmatch(value):
            raise HTTPError(400, "invalid Content-Length", close=True)
        length = int(value)
        if length > MAX_BODY_BYTES:
            raise HTTPError(413, "request body too large", close=True)
        return conn.read_exact(length)

    return b""  # no Content-Length, no Transfer-Encoding: a request has no body


def read_request(conn: Connection) -> Request:
    head = conn.read_until(b"\r\n\r\n", MAX_HEAD_BYTES,
                           HTTPError(431, "request head too large", close=True))
    method, target, version, headers = parse_head(head)

    expect = {v.strip().lower() for k, v in headers if k == "expect"}
    has_body = any(k in ("content-length", "transfer-encoding") for k, _ in headers)
    if "100-continue" in expect and version >= (1, 1) and has_body:
        conn.send(b"HTTP/1.1 100 Continue\r\n\r\n")

    body = read_body(conn, version, headers)
    return Request(method, target, version, headers, body)


# --------------------------------------------------------------------------- #
# The calculator
# --------------------------------------------------------------------------- #
def parse_number(name: str, text: str | None) -> int | float:
    if text is None:
        raise HTTPError(400, f"missing parameter '{name}'")
    if len(text) > MAX_NUMBER_CHARS:
        raise HTTPError(400, f"parameter '{name}' is too long")
    if INT_RE.fullmatch(text):
        return int(text)
    if FLOAT_RE.fullmatch(text):
        value = float(text)
        if math.isfinite(value):
            return value
        raise HTTPError(400, f"parameter '{name}' is out of range")
    raise HTTPError(400, f"parameter '{name}' is not a number")


def divide(a: int | float, b: int | float) -> int | float:
    if b == 0:
        raise HTTPError(400, "division by zero")
    if isinstance(a, int) and isinstance(b, int) and a % b == 0:
        return a // b  # exact: keep it an integer, so 9 / 3 is "3", not "3.0"
    return a / b


OPERATIONS = {
    "/add": operator.add,
    "/sub": operator.sub,
    "/mul": operator.mul,
    "/div": divide,
}
ALLOWED_METHODS = ("GET", "HEAD")


def format_number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    if not math.isfinite(value):
        raise HTTPError(400, "result is out of range")
    if value.is_integer() and abs(value) < 1e15:
        return str(int(value))  # 2.5 * 2 -> "5"
    return repr(value)


def parse_query(query: str) -> dict[str, str]:
    params: dict[str, str] = {}
    for pair in query.split("&"):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        key = unquote(key, errors="strict")
        if key in params:
            raise HTTPError(400, f"parameter '{key}' given more than once")
        params[key] = unquote(value, errors="strict")
    return params


def calculate(request: Request) -> tuple[int, str, list[tuple[str, str]]]:
    """Return (status, body, extra headers) for a well-framed request."""
    hosts = request.get_all("host")
    if len(hosts) > 1 or (request.version >= (1, 1) and not hosts):
        raise HTTPError(400, "missing or duplicate Host header")

    target = request.target
    if target.startswith("/"):
        parts = urlsplit(target)
    elif target.lower().startswith(("http://", "https://")):
        parts = urlsplit(target)  # absolute-form, e.g. from a proxy
    else:
        raise HTTPError(400, "malformed request target")

    try:
        path = unquote(parts.path or "/", errors="strict")
        params = parse_query(parts.query)
    except UnicodeDecodeError:
        raise HTTPError(400, "malformed percent-encoding") from None

    operation = OPERATIONS.get(path)
    if operation is None:
        raise HTTPError(404, f"no such operation: {path}")
    if request.method not in ALLOWED_METHODS:
        raise HTTPError(405, f"method {request.method} not allowed")

    a = parse_number("a", params.get("a"))
    b = parse_number("b", params.get("b"))
    try:
        result = operation(a, b)
    except OverflowError:
        raise HTTPError(400, "result is out of range") from None
    return 200, format_number(result), []


# --------------------------------------------------------------------------- #
# Responses
# --------------------------------------------------------------------------- #
def build_response(status: int, body: str, *, keep_alive: bool, request: Request | None,
                   idle_timeout: float, extra: list[tuple[str, str]] = ()) -> bytes:
    payload = body.encode("utf-8")
    headers = [
        ("Date", formatdate(usegmt=True)),
        ("Server", SERVER_NAME),
        ("Content-Type", "text/plain; charset=utf-8"),
        ("Content-Length", str(len(payload))),
    ]
    headers.extend(extra)
    if status == 405:
        headers.append(("Allow", ", ".join(ALLOWED_METHODS)))
    if not keep_alive:
        headers.append(("Connection", "close"))
    else:
        if request is not None and request.version < (1, 1):
            headers.append(("Connection", "keep-alive"))  # HTTP/1.0 needs it spelled out
        headers.append(("Keep-Alive", f"timeout={int(idle_timeout)}"))

    head = f"HTTP/1.1 {status} {REASONS[status]}\r\n"
    head += "".join(f"{name}: {value}\r\n" for name, value in headers)
    head += "\r\n"
    if request is not None and request.method == "HEAD":
        payload = b""  # same headers as GET, no body
    return head.encode("latin-1") + payload


# --------------------------------------------------------------------------- #
# One connection, many requests
# --------------------------------------------------------------------------- #
def close_gracefully(sock: socket.socket) -> None:
    """Close without letting a TCP RST destroy the response we just sent.

    If unread bytes are sitting in our receive buffer when we close(), most
    stacks answer with RST and the client may never see our last response.
    So: stop writing, drain what the client is still sending, then close.
    """
    try:
        sock.shutdown(socket.SHUT_WR)
        deadline = time.monotonic() + LINGER_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            if not sock.recv(65536):
                break
    except OSError:
        pass
    finally:
        sock.close()


def handle_connection(sock: socket.socket, address, config: "Config") -> None:
    conn = Connection(sock, config.idle_timeout, config.request_timeout)
    peer = f"{address[0]}:{address[1]}"
    served = 0
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while conn.wait_for_request():
            request = None
            try:
                request = read_request(conn)
                keep_alive = request.keep_alive
                try:
                    status, body, extra = calculate(request)
                except HTTPError as error:  # framing was fine, so the connection survives
                    status, body, extra = error.status, error.message, []
            except HTTPError as error:      # framing is broken: answer, then hang up
                status, body, extra, keep_alive = error.status, error.message, [], False
            except ReadTimeout:
                status, body, extra, keep_alive = 408, "request not received in time", [], False
            except PeerClosed:
                # The client half-closed mid-request. It may still be reading.
                status, body, extra, keep_alive = 400, "incomplete request", [], False

            conn.send(build_response(status, body, keep_alive=keep_alive, request=request,
                                     idle_timeout=config.idle_timeout, extra=extra))
            served += 1
            if config.verbose:
                line = f"{request.method} {request.target}" if request else "-"
                print(f"[{peer}] #{served} {line} -> {status}"
                      f"{'' if keep_alive else ' (closing)'}", flush=True)
            if not keep_alive:
                break
    except (OSError, ValueError):
        pass  # connection reset, broken pipe, send timeout ...
    except Exception as error:  # never let one connection take the server down
        print(f"[{peer}] internal error: {error!r}", file=sys.stderr, flush=True)
    finally:
        if config.verbose:
            print(f"[{peer}] closed after {served} response(s)", flush=True)
        close_gracefully(sock)


# --------------------------------------------------------------------------- #
# Listening
# --------------------------------------------------------------------------- #
class Config:
    def __init__(self, idle_timeout: float = DEFAULT_IDLE_TIMEOUT,
                 request_timeout: float = DEFAULT_REQUEST_TIMEOUT, verbose: bool = True):
        self.idle_timeout = idle_timeout
        self.request_timeout = request_timeout
        self.verbose = verbose


class CalculatorServer:
    """Listens on every address ``host`` resolves to (e.g. both ::1 and 127.0.0.1
    for "localhost"), so clients never stall trying the address we skipped."""

    def __init__(self, host: str = "localhost", port: int = 8080, config: Config | None = None):
        self.config = config or Config()
        self.listeners: list[socket.socket] = []
        self.bind_errors: list[str] = []
        self._stop = threading.Event()
        if host == "localhost":
            # Resolvers disagree on what "localhost" means (Windows often says
            # only ::1), so always listen on both loopback addresses.
            targets = [(socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")]
        else:
            infos = socket.getaddrinfo(host or None, port, type=socket.SOCK_STREAM,
                                       flags=socket.AI_PASSIVE)
            targets = list(dict.fromkeys((info[0], info[4][0]) for info in infos))
        last_error: OSError | None = None
        for family, address in targets:
            # With port 0 the first socket picks a free port; the others reuse it.
            bind_port = self.port if self.listeners else port
            try:
                self.listeners.append(socket.create_server((address, bind_port), family=family))
            except OSError as error:  # e.g. IPv6 disabled, or the port is taken on one address
                last_error = error
                self.bind_errors.append(f"{address}:{bind_port} ({error.strerror or error})")
        if not self.listeners:
            raise last_error or OSError(f"could not listen on {host}:{port}")

    @property
    def port(self) -> int:
        return self.listeners[0].getsockname()[1]

    @property
    def addresses(self) -> list[str]:
        out = []
        for listener in self.listeners:
            host, port = listener.getsockname()[:2]
            out.append(f"[{host}]:{port}" if ":" in host else f"{host}:{port}")
        return out

    def serve_forever(self) -> None:
        selector = selectors.DefaultSelector()
        for listener in self.listeners:
            listener.setblocking(False)
            selector.register(listener, selectors.EVENT_READ)
        try:
            while not self._stop.is_set():
                # A short select timeout keeps Ctrl+C responsive on Windows.
                for key, _ in selector.select(timeout=0.5):
                    try:
                        client, address = key.fileobj.accept()
                    except (BlockingIOError, InterruptedError):
                        continue
                    client.setblocking(True)
                    threading.Thread(target=handle_connection,
                                     args=(client, address, self.config),
                                     daemon=True).start()
        finally:
            selector.close()
            for listener in self.listeners:
                listener.close()

    def shutdown(self) -> None:
        self._stop.set()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Persistent-connection HTTP/1.1 calculator")
    parser.add_argument("--host", default="localhost",
                        help="address to listen on (default: localhost; use 0.0.0.0 for all)")
    parser.add_argument("--port", type=int, default=8080, help="port (default: 8080)")
    parser.add_argument("--idle-timeout", type=float, default=DEFAULT_IDLE_TIMEOUT,
                        help="seconds an idle kept-alive connection stays open (default: 10)")
    parser.add_argument("--request-timeout", type=float, default=DEFAULT_REQUEST_TIMEOUT,
                        help="seconds a client has to send one complete request (default: 30)")
    parser.add_argument("--quiet", action="store_true", help="do not log requests")
    args = parser.parse_args(argv)

    config = Config(args.idle_timeout, args.request_timeout, verbose=not args.quiet)
    server = CalculatorServer(args.host, args.port, config)
    for failure in server.bind_errors:
        print(f"warning: could not listen on {failure}", file=sys.stderr, flush=True)
    print(f"calc11 listening on {', '.join(server.addresses)} "
          f"(idle timeout {args.idle_timeout:g}s) - Ctrl+C to stop", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down", flush=True)


if __name__ == "__main__":
    main()
