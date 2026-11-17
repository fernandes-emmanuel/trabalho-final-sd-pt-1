from mpcomm.coordinator.analysis import analyze, count_divergent_positions, count_fifo_violations
from mpcomm.coordinator.report import render, to_dict
from mpcomm.model import PeerStats, RoundLog


def make_log(peer_id, delivered, completed=True):
    return RoundLog(1, peer_id, completed, tuple(delivered), PeerStats())


def test_fifo_violation_is_counted_when_a_sender_goes_backwards():
    assert count_fifo_violations([(0, 0), (1, 0), (0, 1), (1, 1)]) == 0
    assert count_fifo_violations([(0, 1), (0, 0), (1, 0)]) == 1


def test_divergent_positions_cover_every_peer_and_every_position():
    sequences = [
        [(0, 0), (1, 0), (2, 0)],
        [(0, 0), (1, 0), (2, 0)],
        [(0, 0), (1, 0), (0, 1)],
    ]
    assert count_divergent_positions(sequences) == (1, 3)


def test_shorter_log_counts_as_divergent():
    assert count_divergent_positions([[(0, 0), (1, 0)], [(0, 0)]]) == (1, 2)


def test_complete_fifo_round_is_reliable_but_not_totally_ordered():
    logs = {
        0: make_log(0, [(0, 0), (1, 0), (0, 1), (1, 1)]),
        1: make_log(1, [(1, 0), (0, 0), (1, 1), (0, 1)]),
    }
    report = analyze(1, [0, 1], 2, logs)

    assert report.reliable
    assert report.fifo_ordered
    assert not report.totally_ordered
    assert report.divergent_positions == 4


def test_missing_duplicated_and_absent_logs_make_the_round_unreliable():
    logs = {
        0: make_log(0, [(0, 0), (0, 0), (1, 0)]),
        1: make_log(1, [(0, 0), (1, 0)], completed=False),
    }
    report = analyze(1, [0, 1, 2], 1, logs)

    first, second = report.peers
    assert first.duplicates == 1
    assert first.missing == 1
    assert not second.completed
    assert report.absent_peers == (2,)
    assert not report.reliable


def test_report_rendering_and_serialization():
    logs = {0: make_log(0, [(0, 0)])}
    report = analyze(1, [0], 1, logs)

    text = render(report)
    data = to_dict(report)

    assert "Reliable delivery : OK" in text
    assert "FIFO order        : OK" in text
    assert data["reliable"] and data["fifo_ordered"] and data["totally_ordered"]
