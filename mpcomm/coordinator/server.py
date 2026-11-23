from __future__ import annotations

import itertools
import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import grpc

from mpcomm.coordinator.analysis import RoundReport, analyze
from mpcomm.coordinator.report import render, to_dict
from mpcomm.model import Endpoint, Peer, RoundLog
from mpcomm.rpc import mpcomm_pb2 as pb
from mpcomm.rpc import mpcomm_pb2_grpc as rpc
from mpcomm.rpc.clients import GroupManagerClient, PeerControlClient
from mpcomm.rpc.convert import round_log_from_proto
from mpcomm.rpc.server import start_server, stop_server

log = logging.getLogger(__name__)

PEER_POLL_INTERVAL = 0.5
NOT_READY_YET = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})


class CoordinatorError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CoordinatorSettings:
    listen: Endpoint
    advertise_host: str
    group_manager: Endpoint
    expected_peers: int
    startup_timeout: float = 60.0
    round_timeout: float = 180.0


class LogCollector(rpc.ComparisonServicer):
    def __init__(self) -> None:
        self._logs: dict[int, dict[int, RoundLog]] = defaultdict(dict)
        self._arrived = threading.Condition()

    def SubmitLog(self, request: pb.RoundLog, context: grpc.ServicerContext):
        round_log = round_log_from_proto(request)
        log.info("received log from peer %s for round %s", round_log.peer_id, round_log.round_id)
        with self._arrived:
            self._logs[round_log.round_id][round_log.peer_id] = round_log
            self._arrived.notify_all()
        return pb.Empty()

    def collect(self, round_id: int, peer_ids: set[int], timeout: float) -> dict[int, RoundLog]:
        with self._arrived:
            self._arrived.wait_for(lambda: peer_ids <= self._logs[round_id].keys(), timeout)
            return self._logs.pop(round_id, {})


class ExperimentCoordinator:
    def __init__(self, settings: CoordinatorSettings) -> None:
        self._settings = settings
        self._collector = LogCollector()
        self._group_manager = GroupManagerClient(settings.group_manager)
        self._round_ids = itertools.count(int(time.time()))
        self._server: grpc.Server | None = None
        self._report_to = Endpoint(settings.advertise_host, settings.listen.port)

    def __enter__(self) -> ExperimentCoordinator:
        self._server, port = start_server(
            self._settings.listen,
            lambda s: rpc.add_ComparisonServicer_to_server(self._collector, s),
        )
        self._report_to = Endpoint(self._settings.advertise_host, port)
        log.info("coordinator collecting logs at %s", self._report_to)
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._server is not None:
            stop_server(self._server)
        self._group_manager.close()

    def wait_for_peers(self) -> list[Peer]:
        deadline = time.monotonic() + self._settings.startup_timeout
        peers = self._list_peers()
        while len(peers) < self._settings.expected_peers:
            if time.monotonic() > deadline:
                raise CoordinatorError(
                    f"only {len(peers)} of {self._settings.expected_peers} peers registered"
                )
            time.sleep(PEER_POLL_INTERVAL)
            peers = self._list_peers()
        return peers

    def run_round(self, messages_per_peer: int) -> RoundReport:
        peers = self.wait_for_peers()
        round_id = next(self._round_ids)
        log.info("starting round %s with peers %s", round_id, [p.peer_id for p in peers])
        for peer in peers:
            try:
                with PeerControlClient(peer.control_endpoint) as control:
                    control.start(round_id, messages_per_peer, self._report_to)
            except grpc.RpcError as error:
                raise CoordinatorError(
                    f"peer {peer.peer_id} refused to start: {error.details() or error.code()}"
                ) from error
        group = [peer.peer_id for peer in peers]
        logs = self._collector.collect(round_id, set(group), self._settings.round_timeout)
        return analyze(round_id, group, messages_per_peer, logs)

    def shutdown_group(self) -> None:
        for peer in self._list_peers():
            try:
                with PeerControlClient(peer.control_endpoint) as control:
                    control.shutdown()
            except grpc.RpcError as error:
                log.warning("could not stop peer %s: %s", peer.peer_id, error.code())
        try:
            self._group_manager.shutdown()
        except grpc.RpcError as error:
            log.warning("could not stop group manager: %s", error.code())

    def _list_peers(self) -> list[Peer]:
        try:
            return self._group_manager.list_peers()
        except grpc.RpcError as error:
            if error.code() not in NOT_READY_YET:
                raise
            return []


def run_batch(
    settings: CoordinatorSettings,
    messages_per_peer: int,
    rounds: int,
    report_path: Path | None,
    keep_group: bool,
) -> int:
    reports: list[RoundReport] = []
    with ExperimentCoordinator(settings) as coordinator:
        try:
            for _ in range(rounds):
                report = coordinator.run_round(messages_per_peer)
                print(render(report), flush=True)
                reports.append(report)
        finally:
            if not keep_group:
                coordinator.shutdown_group()
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps([to_dict(r) for r in reports], indent=2))
    return 0 if all(r.reliable and r.fifo_ordered for r in reports) else 1


def run_interactive(settings: CoordinatorSettings) -> int:
    with ExperimentCoordinator(settings) as coordinator:
        while (messages := _prompt_messages()) > 0:
            try:
                print(render(coordinator.run_round(messages)), flush=True)
            except CoordinatorError as error:
                log.error("%s", error)
        coordinator.shutdown_group()
    return 0


def _prompt_messages() -> int:
    while True:
        try:
            answer = input("Number of messages for each peer to send (0 to terminate) => ")
        except EOFError:
            return 0
        if answer.strip().isdigit():
            return int(answer)
        print("Please type a non-negative integer.")
