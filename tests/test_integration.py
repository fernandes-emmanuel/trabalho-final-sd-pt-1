import json

import pytest

from mpcomm.launcher import LocalExperiment, run_local
from mpcomm.transport.faults import FaultProfile

pytestmark = pytest.mark.integration


def run(tmp_path, base_port, faults, rounds=1):
    report_path = tmp_path / "report.json"
    experiment = LocalExperiment(
        peers=4,
        messages=15,
        rounds=rounds,
        base_port=base_port,
        faults=faults,
        report=report_path,
        timeout=180,
    )
    exit_code = run_local(experiment)
    return exit_code, json.loads(report_path.read_text())


def test_processes_exchange_every_message_over_a_reliable_network(tmp_path):
    exit_code, reports = run(tmp_path, 7300, FaultProfile())

    assert exit_code == 0
    assert [report["reliable"] and report["fifo_ordered"] for report in reports] == [True]


def test_lossy_network_still_delivers_everything_in_fifo_order(tmp_path):
    faults = FaultProfile(loss=0.25, duplication=0.05, delay_ms=5, jitter_ms=15, seed=7)
    exit_code, reports = run(tmp_path, 7500, faults, rounds=2)

    assert exit_code == 0
    assert len(reports) == 2
    for report in reports:
        assert report["reliable"]
        assert report["fifo_ordered"]
        assert sum(peer["stats"]["retransmissions"] for peer in report["peers"]) > 0
        assert sum(peer["stats"]["dropped_by_fault_injector"] for peer in report["peers"]) > 0
