"""Compares the original socket + pickle protocol with the gRPC GroupManager service."""

from __future__ import annotations

import argparse
import pickle
import socket
import statistics
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from mpcomm.membership.group_manager import GroupManagerService, GroupRegistry
from mpcomm.model import Endpoint
from mpcomm.rpc import mpcomm_pb2 as pb
from mpcomm.rpc import mpcomm_pb2_grpc as rpc
from mpcomm.rpc.clients import GroupManagerClient
from mpcomm.rpc.convert import peer_to_proto
from mpcomm.rpc.server import start_server, stop_server

LOCALHOST = "127.0.0.1"
LEGACY_BUFFER = 2048


@dataclass(frozen=True)
class Measurement:
    name: str
    peers: int
    calls: int
    failures: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    request_bytes: int
    response_bytes: int


class LegacySocketGroupManager:
    def __init__(self, addresses: list[str]) -> None:
        self._addresses = addresses
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((LOCALHOST, 0))
        self._server.listen(64)
        self.port = self._server.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while True:
            try:
                connection, _ = self._server.accept()
            except OSError:
                return
            with connection:
                request = pickle.loads(connection.recv(LEGACY_BUFFER))
                if request["op"] == "list":
                    connection.send(pickle.dumps(self._addresses))

    def close(self) -> None:
        self._server.close()


def legacy_list(port: int) -> list[str]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((LOCALHOST, port))
        client.send(pickle.dumps({"op": "list"}))
        return pickle.loads(client.recv(LEGACY_BUFFER))


def measure(name: str, peers: int, calls: int, call: Callable[[], object], sizes) -> Measurement:
    latencies, failures = [], 0
    for _ in range(calls):
        started = time.perf_counter()
        try:
            call()
        except Exception:
            failures += 1
            continue
        latencies.append((time.perf_counter() - started) * 1000)
    latencies = latencies or [float("nan")]
    quantiles = statistics.quantiles(latencies, n=20) if len(latencies) > 1 else latencies * 19
    return Measurement(
        name=name,
        peers=peers,
        calls=calls,
        failures=failures,
        mean_ms=statistics.fmean(latencies),
        p50_ms=statistics.median(latencies),
        p95_ms=quantiles[18],
        request_bytes=sizes[0],
        response_bytes=sizes[1],
    )


def benchmark(peers: int, calls: int) -> list[Measurement]:
    addresses = [f"10.0.{index // 250}.{index % 250 + 1}" for index in range(peers)]

    legacy = LegacySocketGroupManager(addresses)
    legacy_sizes = (len(pickle.dumps({"op": "list"})), len(pickle.dumps(addresses)))

    registry = GroupRegistry()
    for address in addresses:
        registry.register(address, 6789, 5679)
    service = GroupManagerService(registry, threading.Event())
    server, port = start_server(
        Endpoint(LOCALHOST, 0), lambda s: rpc.add_GroupManagerServicer_to_server(service, s)
    )
    endpoint = Endpoint(LOCALHOST, port)
    reply = pb.PeerList(peers=[peer_to_proto(peer) for peer in registry.peers()])
    grpc_sizes = (pb.Empty().ByteSize(), reply.ByteSize())

    def grpc_new_channel() -> object:
        with GroupManagerClient(endpoint) as client:
            return client.list_peers()

    persistent = GroupManagerClient(endpoint)
    persistent.list_peers()
    try:
        return [
            measure("socket + pickle (original)", peers, calls,
                    lambda: legacy_list(legacy.port), legacy_sizes),
            measure("gRPC, new channel per call", peers, calls, grpc_new_channel, grpc_sizes),
            measure("gRPC, persistent channel", peers, calls, persistent.list_peers, grpc_sizes),
        ]  # fmt: skip
    finally:
        persistent.close()
        legacy.close()
        stop_server(server)


def to_markdown(results: list[Measurement]) -> str:
    lines = [
        "| Implementation | Peers | Calls | Failures | Mean (ms) | p50 (ms) | p95 (ms) "
        "| Request (B) | Response (B) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines += [
        f"| {r.name} | {r.peers} | {r.calls} | {r.failures} | {r.mean_ms:.3f} | {r.p50_ms:.3f} "
        f"| {r.p95_ms:.3f} | {r.request_bytes} | {r.response_bytes} |"
        for r in results
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calls", type=int, default=2000)
    parser.add_argument("--peers", type=int, nargs="+", default=[6, 300])
    args = parser.parse_args()
    results = [m for peers in args.peers for m in benchmark(peers, args.calls)]
    print(to_markdown(results))


if __name__ == "__main__":
    main()
