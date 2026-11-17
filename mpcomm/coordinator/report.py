from __future__ import annotations

import dataclasses
from typing import Any

from mpcomm.coordinator.analysis import RoundReport

COLUMNS = (
    ("peer", 4),
    ("delivered", 9),
    ("missing", 7),
    ("dup", 3),
    ("fifo_viol", 9),
    ("sent", 4),
    ("retrans", 7),
    ("acks", 5),
    ("held", 4),
    ("dropped", 7),
)


def render(report: RoundReport) -> str:
    header = "  ".join(name.rjust(width) for name, width in COLUMNS)
    lines = [
        f"Round {report.round_id}: {len(report.group)} peers x "
        f"{report.messages_per_peer} messages ({report.expected_per_peer} per peer)",
        header,
        "-" * len(header),
    ]
    for verdict in report.peers:
        stats = verdict.stats
        values = (
            verdict.peer_id,
            f"{verdict.delivered}/{report.expected_per_peer}",
            verdict.missing,
            verdict.duplicates,
            verdict.fifo_violations,
            stats.data_sent,
            stats.retransmissions,
            stats.acks_sent,
            stats.held_back,
            stats.dropped_by_fault_injector,
        )
        lines.append(
            "  ".join(
                str(value).rjust(width) for value, (_, width) in zip(values, COLUMNS, strict=True)
            )
        )
    if report.absent_peers:
        lines.append(f"No log received from peers {list(report.absent_peers)}")
    lines += [
        "",
        f"Reliable delivery : {_verdict(report.reliable)}",
        f"FIFO order        : {_verdict(report.fifo_ordered)}",
        f"Total order       : {report.divergent_positions} of {report.compared_positions} "
        "positions differ across peers",
    ]
    return "\n".join(lines)


def to_dict(report: RoundReport) -> dict[str, Any]:
    return {
        **dataclasses.asdict(report),
        "reliable": report.reliable,
        "fifo_ordered": report.fifo_ordered,
        "totally_ordered": report.totally_ordered,
    }


def _verdict(ok: bool) -> str:
    return "OK" if ok else "FAILED"
