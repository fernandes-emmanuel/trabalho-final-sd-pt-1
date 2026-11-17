from __future__ import annotations

from types import TracebackType
from typing import Self

import grpc

from mpcomm.model import Endpoint, Peer, RoundLog
from mpcomm.rpc import mpcomm_pb2 as pb
from mpcomm.rpc import mpcomm_pb2_grpc as rpc
from mpcomm.rpc.convert import peer_from_proto, round_log_to_proto

DEFAULT_TIMEOUT = 5.0


class _Client:
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._channel = grpc.insecure_channel(str(endpoint))
        self._timeout = timeout

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class GroupManagerClient(_Client):
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.GroupManagerStub(self._channel)

    def register(self, host: str, udp_port: int, control_port: int, wait: float = 0.0) -> int:
        request = pb.RegisterRequest(host=host, udp_port=udp_port, control_port=control_port)
        reply = self._stub.Register(
            request, timeout=max(wait, self._timeout), wait_for_ready=wait > 0
        )
        return reply.peer_id

    def unregister(self, peer_id: int) -> None:
        self._stub.Unregister(pb.UnregisterRequest(peer_id=peer_id), timeout=self._timeout)

    def list_peers(self) -> list[Peer]:
        reply = self._stub.ListPeers(pb.Empty(), timeout=self._timeout)
        return [peer_from_proto(peer) for peer in reply.peers]

    def shutdown(self) -> None:
        self._stub.Shutdown(pb.Empty(), timeout=self._timeout)


class PeerControlClient(_Client):
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.PeerControlStub(self._channel)

    def start(self, round_id: int, messages_per_peer: int, report_to: Endpoint) -> int:
        request = pb.StartRequest(
            round_id=round_id, messages_per_peer=messages_per_peer, report_to=str(report_to)
        )
        return self._stub.Start(request, timeout=self._timeout).group_size

    def shutdown(self) -> None:
        self._stub.Shutdown(pb.Empty(), timeout=self._timeout)


class ComparisonClient(_Client):
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.ComparisonStub(self._channel)

    def submit_log(self, log: RoundLog) -> None:
        self._stub.SubmitLog(round_log_to_proto(log), timeout=self._timeout)
