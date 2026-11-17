from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass

from mpcomm.model import Address, Delivery, PeerStats
from mpcomm.transport.channel import Channel
from mpcomm.transport.messages import DataPacket, Kind, Packet
from mpcomm.transport.reliable import PeriodicTask, ReliableFifoMulticast, RetransmissionPolicy

log = logging.getLogger(__name__)

RETRANSMISSION_TICK = 0.01


@dataclass(frozen=True, slots=True)
class RoundTiming:
    min_interval: float = 0.01
    max_interval: float = 0.1
    barrier_timeout: float = 30.0
    completion_timeout: float = 120.0


@dataclass(frozen=True, slots=True)
class RoundResult:
    completed: bool
    delivered: tuple[Delivery, ...]
    stats: PeerStats


class MulticastRound:
    """One experiment round: handshake barrier, data messages and a reliable end marker.

    HELLO and END travel through the same reliable FIFO protocol as the data, so a lost
    handshake or end marker is retransmitted instead of blocking the round forever, and
    delivering END from a sender implies every earlier message from it was delivered.
    """

    def __init__(
        self,
        *,
        round_id: int,
        self_id: int,
        members: Mapping[int, Address],
        channel: Channel,
        messages: int,
        timing: RoundTiming,
        policy: RetransmissionPolicy | None = None,
    ) -> None:
        self.round_id = round_id
        self._self_id = self_id
        self._group = frozenset(members)
        self._messages = messages
        self._timing = timing
        self._random = random.Random()
        self._lock = threading.Lock()
        self._delivered: list[Delivery] = []
        self._greeted: set[int] = set()
        self._finished: set[int] = set()
        self._everyone_ready = threading.Event()
        self._everyone_finished = threading.Event()
        self._protocol = ReliableFifoMulticast(
            round_id=round_id,
            self_id=self_id,
            members=members,
            channel=channel,
            deliver=self._on_deliver,
            policy=policy,
        )
        self._retransmitter = PeriodicTask(
            RETRANSMISSION_TICK, self._protocol.retransmit_due, f"retransmitter-{round_id}"
        )

    def handle(self, packet: Packet) -> None:
        self._protocol.handle(packet)

    def run(self) -> RoundResult:
        self._retransmitter.start()
        try:
            completed = self._exchange()
        except Exception:
            log.exception("round %s aborted", self.round_id)
            completed = False
        finally:
            self._retransmitter.stop()
        return RoundResult(completed, self._snapshot(), self._stats())

    def _exchange(self) -> bool:
        self._protocol.multicast(Kind.HELLO)
        if not self._everyone_ready.wait(self._timing.barrier_timeout):
            log.error("round %s: handshake barrier timed out", self.round_id)
            return False
        log.info("round %s: all %s peers ready", self.round_id, len(self._group))
        for number in range(self._messages):
            time.sleep(self._random.uniform(self._timing.min_interval, self._timing.max_interval))
            self._protocol.multicast(Kind.DATA, number)
        self._protocol.multicast(Kind.END)
        deadline = time.monotonic() + self._timing.completion_timeout
        finished = self._everyone_finished.wait(self._timing.completion_timeout)
        acknowledged = self._protocol.wait_until_acknowledged(max(0.0, deadline - time.monotonic()))
        if not (finished and acknowledged):
            log.error("round %s: did not complete in time", self.round_id)
        return finished and acknowledged and not self._protocol.unreachable

    def _on_deliver(self, packet: DataPacket) -> None:
        with self._lock:
            match packet.kind:
                case Kind.HELLO:
                    self._greeted.add(packet.sender)
                    if self._greeted >= self._group:
                        self._everyone_ready.set()
                case Kind.DATA:
                    self._delivered.append((packet.sender, packet.payload))
                    log.debug("delivered message %s from %s", packet.payload, packet.sender)
                case Kind.END:
                    self._finished.add(packet.sender)
                    if self._finished >= self._group:
                        self._everyone_finished.set()

    def _snapshot(self) -> tuple[Delivery, ...]:
        with self._lock:
            return tuple(self._delivered)

    def _stats(self) -> PeerStats:
        counters = self._protocol.counters
        return PeerStats(
            data_sent=counters.data_sent,
            retransmissions=counters.retransmissions,
            acks_sent=counters.acks_sent,
            duplicates_discarded=counters.duplicates_discarded,
            held_back=counters.held_back,
            unreachable_peers=tuple(sorted(self._protocol.unreachable)),
        )
