from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from mpcomm.model import Delivery, PeerStats, RoundLog


@dataclass(frozen=True, slots=True)
class PeerVerdict:
    peer_id: int
    completed: bool
    delivered: int
    missing: int
    duplicates: int
    fifo_violations: int
    stats: PeerStats


@dataclass(frozen=True, slots=True)
class RoundReport:
    round_id: int
    group: tuple[int, ...]
    messages_per_peer: int
    peers: tuple[PeerVerdict, ...]
    absent_peers: tuple[int, ...]
    divergent_positions: int
    compared_positions: int

    @property
    def expected_per_peer(self) -> int:
        return len(self.group) * self.messages_per_peer

    @property
    def reliable(self) -> bool:
        return not self.absent_peers and all(
            verdict.completed and verdict.missing == 0 and verdict.duplicates == 0
            for verdict in self.peers
        )

    @property
    def fifo_ordered(self) -> bool:
        return all(verdict.fifo_violations == 0 for verdict in self.peers)

    @property
    def totally_ordered(self) -> bool:
        return self.reliable and self.divergent_positions == 0


def analyze(
    round_id: int,
    group: Sequence[int],
    messages_per_peer: int,
    logs: Mapping[int, RoundLog],
) -> RoundReport:
    expected = {(sender, number) for sender in group for number in range(messages_per_peer)}
    verdicts = tuple(
        _judge(logs[peer_id], expected) for peer_id in sorted(logs) if peer_id in group
    )
    divergent, compared = count_divergent_positions(
        [logs[peer_id].delivered for peer_id in sorted(logs)]
    )
    return RoundReport(
        round_id=round_id,
        group=tuple(group),
        messages_per_peer=messages_per_peer,
        peers=verdicts,
        absent_peers=tuple(peer_id for peer_id in group if peer_id not in logs),
        divergent_positions=divergent,
        compared_positions=compared,
    )


def count_fifo_violations(delivered: Sequence[Delivery]) -> int:
    highest: dict[int, int] = {}
    violations = 0
    for sender, number in delivered:
        if number < highest.get(sender, -1):
            violations += 1
        highest[sender] = max(number, highest.get(sender, -1))
    return violations


def count_divergent_positions(sequences: Sequence[Sequence[Delivery]]) -> tuple[int, int]:
    if not sequences:
        return 0, 0
    compared = max(len(sequence) for sequence in sequences)
    divergent = sum(
        1
        for position in range(compared)
        if len({_at(sequence, position) for sequence in sequences}) > 1
    )
    return divergent, compared


def _judge(log: RoundLog, expected: set[Delivery]) -> PeerVerdict:
    occurrences = Counter(log.delivered)
    return PeerVerdict(
        peer_id=log.peer_id,
        completed=log.completed,
        delivered=len(log.delivered),
        missing=len(expected - occurrences.keys()),
        duplicates=sum(count - 1 for count in occurrences.values()),
        fifo_violations=count_fifo_violations(log.delivered),
        stats=log.stats,
    )


def _at(sequence: Sequence[Delivery], position: int) -> Delivery | None:
    return sequence[position] if position < len(sequence) else None
