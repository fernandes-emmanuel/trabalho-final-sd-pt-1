from __future__ import annotations

import dataclasses
import logging
import threading
from dataclasses import dataclass, field

import grpc

from mpcomm.model import Address, Endpoint, RoundLog
from mpcomm.peer.round import MulticastRound, RoundTiming
from mpcomm.rpc import mpcomm_pb2 as pb
from mpcomm.rpc import mpcomm_pb2_grpc as rpc
from mpcomm.rpc.clients import ComparisonClient, GroupManagerClient
from mpcomm.rpc.server import start_server, stop_server, wait_until_stopped
from mpcomm.transport.channel import Channel
from mpcomm.transport.faults import FaultProfile, FaultyChannel
from mpcomm.transport.messages import AckPacket, DataPacket, MalformedPacket, decode, encode
from mpcomm.transport.udp import UdpEndpoint

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PeerSettings:
    group_manager: Endpoint
    bind_host: str
    advertise_host: str
    udp_port: int
    control_port: int
    faults: FaultProfile = field(default_factory=FaultProfile)
    timing: RoundTiming = field(default_factory=RoundTiming)
    registration_timeout: float = 30.0


class RoundRejected(Exception):
    pass


class PeerNode:
    def __init__(self, settings: PeerSettings) -> None:
        self._settings = settings
        self._endpoint = UdpEndpoint(settings.bind_host, settings.udp_port)
        self._faulty = (
            FaultyChannel(self._endpoint, settings.faults) if settings.faults.active else None
        )
        self._channel: Channel = self._faulty or self._endpoint
        self._group_manager = GroupManagerClient(settings.group_manager)
        self._lock = threading.Lock()
        self._current: MulticastRound | None = None
        self._running = False
        self._members: dict[int, Address] = {}
        self._finished_rounds: set[int] = set()
        self._stop = threading.Event()
        self.peer_id = -1

    def run(self) -> None:
        self._endpoint.serve(self._on_datagram)
        listen = Endpoint(self._settings.bind_host, self._settings.control_port)
        server, _ = start_server(
            listen, lambda s: rpc.add_PeerControlServicer_to_server(PeerControlService(self), s)
        )
        try:
            self.peer_id = self._group_manager.register(
                self._settings.advertise_host,
                self._endpoint.port,
                self._settings.control_port,
                wait=self._settings.registration_timeout,
            )
            log.info(
                "peer %s registered as %s:%s",
                self.peer_id,
                self._settings.advertise_host,
                self._endpoint.port,
            )
            wait_until_stopped(self._stop)
        finally:
            self._leave_group()
            stop_server(server)
            self._close_transport()
        log.info("peer %s stopped", self.peer_id)

    def stop(self) -> None:
        self._stop.set()

    def begin_round(self, round_id: int, messages: int, report_to: Endpoint) -> int:
        with self._lock:
            if self._running:
                raise RoundRejected("a round is already running")
            if round_id in self._finished_rounds:
                raise RoundRejected(f"round {round_id} already finished")
            self._running = True
        try:
            members = {peer.peer_id: peer.udp_address for peer in self._group_manager.list_peers()}
            if self.peer_id not in members:
                raise RoundRejected(f"peer {self.peer_id} is not registered in the group")
            current = MulticastRound(
                round_id=round_id,
                self_id=self.peer_id,
                members=members,
                channel=self._channel,
                messages=messages,
                timing=self._settings.timing,
            )
        except BaseException:
            with self._lock:
                self._running = False
            raise
        with self._lock:
            self._current = current
            self._members = members
        threading.Thread(
            target=self._run_round, args=(current, report_to), name=f"round-{round_id}", daemon=True
        ).start()
        log.info("peer %s starting round %s with %s peers", self.peer_id, round_id, len(members))
        return len(members)

    def _run_round(self, current: MulticastRound, report_to: Endpoint) -> None:
        dropped_before = self._dropped()
        result = current.run()
        stats = dataclasses.replace(
            result.stats, dropped_by_fault_injector=self._dropped() - dropped_before
        )
        with self._lock:
            self._finished_rounds.add(current.round_id)
            self._running = False
        log.info(
            "peer %s finished round %s: %s messages delivered, %s retransmissions",
            self.peer_id,
            current.round_id,
            len(result.delivered),
            stats.retransmissions,
        )
        report = RoundLog(current.round_id, self.peer_id, result.completed, result.delivered, stats)
        try:
            with ComparisonClient(report_to) as coordinator:
                coordinator.submit_log(report)
        except grpc.RpcError as error:
            log.error("could not submit log to %s: %s", report_to, error.code())

    def _on_datagram(self, data: bytes, address: Address) -> None:
        try:
            packet = decode(data)
        except MalformedPacket as error:
            log.warning("discarding malformed datagram from %s: %s", address, error)
            return
        with self._lock:
            current = self._current
            stale = packet.round_id in self._finished_rounds
            sender_address = self._members.get(getattr(packet, "sender", -1))
        if current is not None and packet.round_id == current.round_id:
            current.handle(packet)
        elif stale and isinstance(packet, DataPacket):
            self._acknowledge_stale(packet, sender_address)

    def _acknowledge_stale(self, packet: DataPacket, address: Address | None) -> None:
        if address is not None:
            ack = AckPacket(packet.round_id, self.peer_id, packet.sender, packet.seq)
            self._channel.send(encode(ack), address)

    def _dropped(self) -> int:
        return self._faulty.dropped if self._faulty else 0

    def _leave_group(self) -> None:
        if self.peer_id < 0:
            return
        try:
            self._group_manager.unregister(self.peer_id)
        except grpc.RpcError as error:
            log.warning("could not unregister from group manager: %s", error.code())
        finally:
            self._group_manager.close()

    def _close_transport(self) -> None:
        self._endpoint.close()
        if self._faulty is not None:
            self._faulty.close()


class PeerControlService(rpc.PeerControlServicer):
    def __init__(self, node: PeerNode) -> None:
        self._node = node

    def Start(self, request: pb.StartRequest, context: grpc.ServicerContext):
        if request.messages_per_peer <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "messages_per_peer must be positive")
        try:
            report_to = Endpoint.parse(request.report_to)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        try:
            group_size = self._node.begin_round(
                request.round_id, request.messages_per_peer, report_to
            )
        except RoundRejected as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        except grpc.RpcError as error:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"group manager unreachable: {error.code()}")
        return pb.StartReply(peer_id=self._node.peer_id, group_size=group_size)

    def Shutdown(self, request: pb.Empty, context: grpc.ServicerContext):
        self._node.stop()
        return pb.Empty()
