"""End-to-end tests: a real server on a real socket.  Run:  python -m unittest -v"""
from __future__ import annotations

import socket
import threading
import time
import unittest

from grade import CHECKS, ResponseReader, build_request, socket_is_open
from server import CalculatorServer, Config

IDLE_TIMEOUT = 1.0


class ServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = CalculatorServer("127.0.0.1", 0, Config(IDLE_TIMEOUT, 3.0, verbose=False))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(timeout=5)

    def connect(self) -> tuple[socket.socket, ResponseReader]:
        sock = socket.create_connection(("127.0.0.1", self.server.port))
        sock.settimeout(5)
        self.addCleanup(sock.close)
        return sock, ResponseReader(sock)

    def exchange(self, raw: bytes, head_request: bool = False):
        sock, reader = self.connect()
        sock.sendall(raw)
        return reader.read(head_request)

    def get(self, target: str):
        status, _, body = self.exchange(build_request("GET", target))
        return status, body.decode()

    def assert_closed_by_server(self, sock: socket.socket):
        sock.settimeout(5)
        self.assertEqual(sock.recv(1), b"", "server should have closed the connection")


class TestFeatureSet(ServerTestCase):
    """The table from the assignment, one request per connection."""

    def test_arithmetic(self):
        self.assertEqual(self.get("/add?a=2&b=3"), (200, "5"))
        self.assertEqual(self.get("/sub?a=10&b=4"), (200, "6"))
        self.assertEqual(self.get("/mul?a=6&b=7"), (200, "42"))
        self.assertEqual(self.get("/div?a=9&b=3"), (200, "3"))

    def test_errors(self):
        self.assertEqual(self.get("/div?a=1&b=0")[0], 400)
        self.assertEqual(self.get("/add?a=x&b=3")[0], 400)
        self.assertEqual(self.get("/pow?a=2&b=8")[0], 404)

    def test_post_is_405_with_allow(self):
        status, headers, _ = self.exchange(build_request("POST", "/add"))
        self.assertEqual(status, 405)
        self.assertEqual(headers["allow"], "GET, HEAD")

    def test_missing_host_is_400(self):
        status, _, _ = self.exchange(b"GET /add?a=2&b=3 HTTP/1.1\r\n\r\n")
        self.assertEqual(status, 400)

    def test_duplicate_host_is_400(self):
        status, _, _ = self.exchange(b"GET /add?a=2&b=3 HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n")
        self.assertEqual(status, 400)

    def test_http10_needs_no_host(self):
        status, _, body = self.exchange(b"GET /add?a=2&b=3 HTTP/1.0\r\n\r\n")
        self.assertEqual((status, body), (200, b"5"))

    def test_numbers(self):
        self.assertEqual(self.get("/div?a=7&b=2"), (200, "3.5"))
        self.assertEqual(self.get("/add?a=-2&b=-3"), (200, "-5"))
        self.assertEqual(self.get("/add?a=%2B2&b=3"), (200, "5"))   # percent-encoded "+"
        self.assertEqual(self.get("/mul?a=2.5&b=2"), (200, "5"))
        self.assertEqual(self.get("/add?a=0.1&b=0.2"), (200, "0.30000000000000004"))
        self.assertEqual(self.get("/mul?a=99999999999999999999&b=10"),
                         (200, "999999999999999999990"))
        self.assertEqual(self.get("/div?a=-9&b=3"), (200, "-3"))
        self.assertEqual(self.get("/div?a=0.0&b=0")[0], 400)

    def test_bad_parameters(self):
        for target in ["/add?a=2", "/add?b=2", "/add", "/add?a=&b=1", "/add?a=1&b=nan",
                       "/add?a=1&b=inf", "/add?a=1_000&b=1", "/add?a=1&a=2&b=3",
                       "/mul?a=1e308&b=1e308", "/add?a=1e999&b=1", "/add?a=%zz&b=1"]:
            with self.subTest(target=target):
                self.assertEqual(self.get(target)[0], 400)

    def test_unknown_paths(self):
        for target in ["/", "/add/", "/ADD?a=1&b=2", "/pow"]:
            with self.subTest(target=target):
                self.assertEqual(self.get(target)[0], 404)

    def test_head_has_length_but_no_body(self):
        sock, reader = self.connect()
        sock.sendall(build_request("HEAD", "/mul?a=6&b=7") + build_request("GET", "/add?a=1&b=1"))
        status, headers, body = reader.read(head_request=True)
        self.assertEqual((status, headers["content-length"], body), (200, "2", b""))
        # If HEAD had leaked a body, it would now be misread as the next response.
        self.assertEqual(reader.read()[2], b"2")

    def test_response_headers(self):
        _, headers, _ = self.exchange(build_request("GET", "/add?a=2&b=3"))
        self.assertEqual(headers["content-length"], "1")
        self.assertTrue(headers["content-type"].startswith("text/plain"))
        self.assertIn("date", headers)


class TestPersistence(ServerTestCase):
    def test_marking_scenario(self):
        """One socket, every request; the socket must still be open afterwards."""
        sock, reader = self.connect()
        for method, target, want_status, want_body in CHECKS:
            sock.sendall(build_request(method, target))
            status, _, body = reader.read()
            self.assertEqual(status, want_status, target)
            if want_body is not None:
                self.assertEqual(body.decode(), want_body)
        self.assertTrue(socket_is_open(sock))
        sock.sendall(build_request("GET", "/add?a=1&b=1"))
        self.assertEqual(reader.read()[2], b"2")

    def test_pipelining_answers_in_order(self):
        sock, reader = self.connect()
        sock.sendall(b"".join(build_request(m, t) for m, t, _, _ in CHECKS))
        self.assertEqual([reader.read()[0] for _ in CHECKS], [c[2] for c in CHECKS])
        self.assertTrue(socket_is_open(sock))

    def test_many_requests_on_one_connection(self):
        sock, reader = self.connect()
        for i in range(200):
            sock.sendall(build_request("GET", f"/add?a={i}&b=1"))
            self.assertEqual(reader.read()[2], str(i + 1).encode())

    def test_request_split_across_many_packets(self):
        sock, reader = self.connect()
        for byte in build_request("GET", "/mul?a=6&b=7"):
            sock.sendall(bytes([byte]))
            time.sleep(0.001)
        self.assertEqual(reader.read()[2], b"42")
        self.assertTrue(socket_is_open(sock))

    def test_leading_blank_lines_are_ignored(self):
        status, _, body = self.exchange(b"\r\n\r\n" + build_request("GET", "/add?a=2&b=3"))
        self.assertEqual((status, body), (200, b"5"))

    def test_connection_close_is_honoured(self):
        sock, reader = self.connect()
        sock.sendall(b"GET /add?a=2&b=3 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
                     + build_request("GET", "/add?a=9&b=9"))
        status, headers, body = reader.read()
        self.assertEqual((status, body, headers["connection"]), (200, b"5", "close"))
        self.assert_closed_by_server(sock)

    def test_http10_closes_by_default(self):
        sock, reader = self.connect()
        sock.sendall(b"GET /add?a=2&b=3 HTTP/1.0\r\n\r\n")
        self.assertEqual(reader.read()[2], b"5")
        self.assert_closed_by_server(sock)

    def test_http10_keep_alive(self):
        sock, reader = self.connect()
        request = b"GET /add?a=2&b=3 HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"
        sock.sendall(request)
        _, headers, _ = reader.read()
        self.assertEqual(headers["connection"], "keep-alive")
        sock.sendall(request)
        self.assertEqual(reader.read()[2], b"5")

    def test_idle_timeout_closes_connection(self):
        sock, reader = self.connect()
        sock.sendall(build_request("GET", "/add?a=2&b=3"))
        _, headers, _ = reader.read()
        self.assertEqual(headers["keep-alive"], f"timeout={int(IDLE_TIMEOUT)}")
        started = time.monotonic()
        self.assert_closed_by_server(sock)
        self.assertLess(time.monotonic() - started, IDLE_TIMEOUT + 2)

    def test_error_statuses_keep_connection_open(self):
        sock, reader = self.connect()
        for target in ["/div?a=1&b=0", "/nope", "/add?a=x&b=1"]:
            sock.sendall(build_request("GET", target))
            self.assertNotEqual(reader.read()[0], 200)
        sock.sendall(b"GET /add?a=1&b=1 HTTP/1.1\r\n\r\n")  # no Host
        self.assertEqual(reader.read()[0], 400)
        sock.sendall(build_request("GET", "/add?a=1&b=1"))
        self.assertEqual(reader.read()[2], b"2")


class TestBodies(ServerTestCase):
    """Byte n+1 belongs to somebody else."""

    def test_content_length_body_is_consumed_exactly(self):
        sock, reader = self.connect()
        body = b"GET /pow?a=1&b=1 HTTP/1.1\r\n\r\n"  # looks like a request, but it is body
        sock.sendall(b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n" % len(body)
                     + body + build_request("GET", "/add?a=2&b=3"))
        self.assertEqual(reader.read()[0], 405)
        self.assertEqual(reader.read()[:3:2], (200, b"5"))
        self.assertTrue(socket_is_open(sock))

    def test_body_arriving_late(self):
        sock, reader = self.connect()
        sock.sendall(b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n01234")
        time.sleep(0.2)
        sock.sendall(b"56789" + build_request("GET", "/sub?a=10&b=4"))
        self.assertEqual(reader.read()[0], 405)
        self.assertEqual(reader.read()[2], b"6")

    def test_get_with_body(self):
        sock, reader = self.connect()
        sock.sendall(b"GET /add?a=2&b=3 HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc"
                     + build_request("GET", "/mul?a=6&b=7"))
        self.assertEqual(reader.read()[2], b"5")
        self.assertEqual(reader.read()[2], b"42")

    def test_chunked_body(self):
        sock, reader = self.connect()
        sock.sendall(b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                     b"5;ext=1\r\nhello\r\nA\r\n0123456789\r\n0\r\nX-Trailer: yes\r\n\r\n"
                     + build_request("GET", "/add?a=2&b=3"))
        self.assertEqual(reader.read()[0], 405)
        self.assertEqual(reader.read()[2], b"5")
        self.assertTrue(socket_is_open(sock))

    def test_expect_100_continue(self):
        sock, reader = self.connect()
        sock.sendall(b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n"
                     b"Expect: 100-continue\r\n\r\n")
        self.assertTrue(sock.recv(64).startswith(b"HTTP/1.1 100 Continue\r\n\r\n"))
        sock.sendall(b"body" + build_request("GET", "/add?a=2&b=3"))
        self.assertEqual(reader.read()[0], 405)
        self.assertEqual(reader.read()[2], b"5")


class TestMalformed(ServerTestCase):
    """When framing is unknowable, answer and close instead of guessing."""

    def assert_rejected_and_closed(self, raw: bytes, status: int):
        sock, reader = self.connect()
        sock.sendall(raw)
        got, headers, _ = reader.read()
        self.assertEqual(got, status)
        self.assertEqual(headers.get("connection"), "close")
        self.assert_closed_by_server(sock)

    def test_cases(self):
        cases = [
            (b"GARBAGE\r\n\r\n", 400),
            (b"GET /add?a=1&b=1\r\n\r\n", 400),
            (b"GET  /add HTTP/1.1\r\nHost: x\r\n\r\n", 400),
            (b"GET /add?a=1&b=1 HTTP/2.0\r\nHost: x\r\n\r\n", 505),
            (b"GET /add HTTP/1.1\r\nHost x\r\n\r\n", 400),
            (b"GET /add HTTP/1.1\r\nHost : x\r\n\r\n", 400),
            (b"GET /add HTTP/1.1\r\nHost: x\r\n folded\r\n\r\n", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n"
             b"Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", 501),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcXX", 400),
            (b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999\r\n\r\n", 413),
            (b"GET /add HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a" * 10000 + b"\r\n\r\n", 431),
        ]
        for raw, status in cases:
            with self.subTest(raw=raw[:60]):
                self.assert_rejected_and_closed(raw, status)

    def test_incomplete_request_times_out_with_408(self):
        sock, reader = self.connect()
        sock.sendall(b"GET /add?a=1&b=1 HTTP/1.1\r\nHost: x\r\n")  # never finished
        self.assertEqual(reader.read()[0], 408)
        self.assert_closed_by_server(sock)

    def test_half_close_mid_request(self):
        sock, reader = self.connect()
        sock.sendall(b"GET /add?a=1&b=1 HTTP/1.1\r\nHo")
        sock.shutdown(socket.SHUT_WR)
        self.assertEqual(reader.read()[0], 400)

    def test_half_close_after_complete_requests(self):
        sock, reader = self.connect()
        sock.sendall(build_request("GET", "/add?a=2&b=3") + build_request("GET", "/mul?a=6&b=7"))
        sock.shutdown(socket.SHUT_WR)
        self.assertEqual(reader.read()[2], b"5")
        self.assertEqual(reader.read()[2], b"42")
        self.assert_closed_by_server(sock)


class TestConcurrency(ServerTestCase):
    def test_parallel_clients(self):
        errors = []

        def client(n):
            try:
                with socket.create_connection(("127.0.0.1", self.server.port), timeout=5) as sock:
                    reader = ResponseReader(sock)
                    for i in range(20):
                        sock.sendall(build_request("GET", f"/mul?a={n}&b={i}"))
                        assert reader.read()[2] == str(n * i).encode()
            except Exception as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=client, args=(n,)) for n in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

    def test_idle_client_does_not_block_others(self):
        idle, _ = self.connect()  # connected, sends nothing
        self.assertEqual(self.get("/add?a=2&b=3"), (200, "5"))
        idle.close()


if __name__ == "__main__":
    unittest.main()
