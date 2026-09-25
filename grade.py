#!/usr/bin/env python3
"""Replay the marking scenario: one socket, every request.

    python grade.py                 # requests one at a time
    python grade.py --pipeline      # all six written at once, answers must come back in order

Start the server first:  python server.py
"""
from __future__ import annotations

import argparse
import select
import socket
import sys

CHECKS = [
    ("GET", "/add?a=2&b=3", 200, "5"),
    ("GET", "/sub?a=10&b=4", 200, "6"),
    ("GET", "/mul?a=6&b=7", 200, "42"),
    ("GET", "/div?a=1&b=0", 400, None),
    ("GET", "/pow?a=2&b=8", 404, None),
    ("POST", "/add", 405, None),
]


def build_request(method: str, target: str, host: str = "localhost") -> bytes:
    return f"{method} {target} HTTP/1.1\r\nHost: {host}\r\n\r\n".encode()


class ResponseReader:
    """Reads responses off a socket using Content-Length, keeping leftovers."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = b""

    def _fill(self) -> None:
        data = self.sock.recv(65536)
        if not data:
            raise ConnectionError("server closed the connection")
        self.buf += data

    def read(self, head_request: bool = False) -> tuple[int, dict[str, str], bytes]:
        while b"\r\n\r\n" not in self.buf:
            self._fill()
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        lines = head.decode("latin-1").split("\r\n")
        status = int(lines[0].split(" ")[1])
        headers = {}
        for line in lines[1:]:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        if status == 100:  # interim response; the real one follows
            return self.read(head_request)
        length = 0 if head_request else int(headers.get("content-length", "0"))
        while len(self.buf) < length:
            self._fill()
        body, self.buf = self.buf[:length], self.buf[length:]
        return status, headers, body


def socket_is_open(sock: socket.socket) -> bool:
    """True if the peer has not closed: nothing readable, or readable data (not EOF)."""
    readable, _, _ = select.select([sock], [], [], 0.2)
    if not readable:
        return True
    try:
        return sock.recv(1, socket.MSG_PEEK) != b""
    except OSError:
        return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--pipeline", action="store_true",
                        help="send all requests before reading any response")
    args = parser.parse_args()

    s = socket.create_connection((args.host, args.port))
    s.settimeout(5)
    reader = ResponseReader(s)
    ok = True

    if args.pipeline:
        s.sendall(b"".join(build_request(m, t) for m, t, _, _ in CHECKS))

    for method, target, want_status, want_body in CHECKS:
        if not args.pipeline:
            s.sendall(build_request(method, target))
        status, _, body = reader.read()
        passed = status == want_status and (want_body is None or body.decode() == want_body)
        ok &= passed
        shown = body.decode() if status == 200 else ""
        print(f"  {'PASS' if passed else 'FAIL'}  {method:<4} {target:<16} -> {status}   {shown}")

    still_open = socket_is_open(s)
    ok &= still_open
    print(f"\n  socket still open: {still_open}")
    print(f"  1 TCP handshake, {len(CHECKS)} responses")
    s.close()
    print("\nALL PASSED" if ok else "\nSOMETHING FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
