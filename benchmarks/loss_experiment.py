"""Runs local experiments under increasing packet loss and summarizes the reports."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from mpcomm.launcher import LocalExperiment, run_local
from mpcomm.transport.faults import FaultProfile


def run_scenario(loss: float, args: argparse.Namespace, workdir: Path) -> dict:
    report_path = workdir / f"loss-{loss}.json"
    experiment = LocalExperiment(
        peers=args.peers,
        messages=args.messages,
        rounds=args.rounds,
        base_port=args.base_port,
        faults=FaultProfile(loss=loss, delay_ms=5, jitter_ms=10, seed=args.seed),
        report=report_path,
        log_level="warning",
    )
    started = time.perf_counter()
    exit_code = run_local(experiment)
    elapsed = time.perf_counter() - started
    reports = json.loads(report_path.read_text())
    peers = [peer for report in reports for peer in report["peers"]]
    expected = sum(report["messages_per_peer"] * len(report["group"]) ** 2 for report in reports)
    return {
        "loss": loss,
        "exit_code": exit_code,
        "delivered": sum(peer["delivered"] for peer in peers),
        "expected": expected,
        "fifo_violations": sum(peer["fifo_violations"] for peer in peers),
        "duplicates": sum(peer["duplicates"] for peer in peers),
        "data_sent": sum(peer["stats"]["data_sent"] for peer in peers),
        "dropped": sum(peer["stats"]["dropped_by_fault_injector"] for peer in peers),
        "retransmissions": sum(peer["stats"]["retransmissions"] for peer in peers),
        "duplicates_discarded": sum(peer["stats"]["duplicates_discarded"] for peer in peers),
        "held_back": sum(peer["stats"]["held_back"] for peer in peers),
        "seconds": elapsed / len(reports),
    }


def to_markdown(rows: list[dict]) -> str:
    lines = [
        "| Loss | Delivered | FIFO violations | Duplicates delivered | Packets dropped "
        "| Retransmissions | Duplicates discarded | Held back | Seconds per round |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines += [
        f"| {row['loss']:.0%} | {row['delivered']}/{row['expected']} | {row['fifo_violations']} "
        f"| {row['duplicates']} | {row['dropped']} | {row['retransmissions']} "
        f"| {row['duplicates_discarded']} | {row['held_back']} | {row['seconds']:.1f} |"
        for row in rows
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--losses", type=float, nargs="+", default=[0.0, 0.1, 0.2, 0.3, 0.4])
    parser.add_argument("--peers", type=int, default=6)
    parser.add_argument("--messages", type=int, default=30)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seed", type=int, default=2024)
    parser.add_argument("--base-port", type=int, default=7700)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as workdir:
        rows = [run_scenario(loss, args, Path(workdir)) for loss in args.losses]
    print(to_markdown(rows))


if __name__ == "__main__":
    main()
