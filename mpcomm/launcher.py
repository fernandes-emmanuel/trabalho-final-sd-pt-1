from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from mpcomm.transport.faults import FaultProfile

log = logging.getLogger(__name__)

LOCALHOST = "127.0.0.1"
PROCESS_EXIT_TIMEOUT = 15.0


@dataclass(frozen=True, slots=True)
class LocalExperiment:
    peers: int
    messages: int
    rounds: int = 1
    base_port: int = 7000
    faults: FaultProfile = field(default_factory=FaultProfile)
    report: Path | None = None
    timeout: float = 300.0
    log_level: str = "info"

    @property
    def group_manager(self) -> str:
        return f"{LOCALHOST}:{self.base_port}"

    @property
    def coordinator_port(self) -> int:
        return self.base_port + 1

    def udp_port(self, index: int) -> int:
        return self.base_port + 100 + index

    def control_port(self, index: int) -> int:
        return self.base_port + 200 + index


def run_local(experiment: LocalExperiment) -> int:
    background: list[subprocess.Popen[bytes]] = []
    try:
        background.append(_spawn(experiment, "group-manager", "--listen", experiment.group_manager))
        for index in range(experiment.peers):
            background.append(_spawn(experiment, "peer", *_peer_arguments(experiment, index)))
        coordinator = subprocess.run(
            _command(experiment, "coordinator", *_coordinator_arguments(experiment)),
            timeout=experiment.timeout,
            check=False,
        )
        for process in background:
            process.wait(PROCESS_EXIT_TIMEOUT)
        return coordinator.returncode
    except subprocess.TimeoutExpired:
        log.error("local experiment did not finish in time")
        return 1
    finally:
        for process in background:
            if process.poll() is None:
                process.kill()


def _peer_arguments(experiment: LocalExperiment, index: int) -> list[str]:
    faults = experiment.faults
    arguments = [
        "--group-manager", experiment.group_manager,
        "--bind", LOCALHOST,
        "--advertise-host", LOCALHOST,
        "--udp-port", str(experiment.udp_port(index)),
        "--control-port", str(experiment.control_port(index)),
        "--loss", str(faults.loss),
        "--duplication", str(faults.duplication),
        "--delay-ms", str(faults.delay_ms),
        "--jitter-ms", str(faults.jitter_ms),
    ]  # fmt: skip
    if faults.seed is not None:
        arguments += ["--seed", str(faults.seed + index)]
    return arguments


def _coordinator_arguments(experiment: LocalExperiment) -> list[str]:
    arguments = [
        "--group-manager", experiment.group_manager,
        "--bind", LOCALHOST,
        "--advertise-host", LOCALHOST,
        "--port", str(experiment.coordinator_port),
        "--peers", str(experiment.peers),
        "--messages", str(experiment.messages),
        "--rounds", str(experiment.rounds),
    ]  # fmt: skip
    if experiment.report is not None:
        arguments += ["--report", str(experiment.report)]
    return arguments


def _spawn(experiment: LocalExperiment, *arguments: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(_command(experiment, *arguments, background=True))


def _command(experiment: LocalExperiment, *arguments: str, background: bool = False) -> list[str]:
    level = "warning" if background else experiment.log_level
    return [sys.executable, "-m", "mpcomm", "--log-level", level, *arguments]
