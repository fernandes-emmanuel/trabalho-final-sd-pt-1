from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
from collections.abc import Sequence
from pathlib import Path

from mpcomm.coordinator.server import (
    CoordinatorError,
    CoordinatorSettings,
    run_batch,
    run_interactive,
)
from mpcomm.launcher import LocalExperiment, run_local
from mpcomm.membership.group_manager import run_group_manager
from mpcomm.model import Endpoint
from mpcomm.peer.node import PeerNode, PeerSettings
from mpcomm.peer.round import RoundTiming
from mpcomm.transport.faults import FaultProfile

DEFAULT_GROUP_MANAGER = "localhost:5680"
DEFAULT_COORDINATOR_PORT = 5678
DEFAULT_PEER_UDP_PORT = 6789
DEFAULT_PEER_CONTROL_PORT = 5679


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    )
    try:
        return args.handler(args)
    except (CoordinatorError, OSError, ValueError) as error:
        logging.getLogger("mpcomm").error("%s", error)
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mpcomm", description="Multicast communication with gRPC and reliable FIFO UDP"
    )
    parser.add_argument("--log-level", default=_env("LOG_LEVEL", "info"))
    commands = parser.add_subparsers(dest="command", required=True)

    group_manager = commands.add_parser("group-manager", help="run the membership service")
    group_manager.add_argument(
        "--listen", type=Endpoint.parse, default=_env("GROUP_MANAGER_LISTEN", "0.0.0.0:5680")
    )
    group_manager.set_defaults(handler=_run_group_manager)

    peer = commands.add_parser("peer", help="run a peer process")
    _add_group_manager_option(peer)
    _add_host_options(peer)
    peer.add_argument(
        "--udp-port", type=int, default=int(_env("UDP_PORT", str(DEFAULT_PEER_UDP_PORT)))
    )
    peer.add_argument(
        "--control-port",
        type=int,
        default=int(_env("CONTROL_PORT", str(DEFAULT_PEER_CONTROL_PORT))),
    )
    peer.add_argument("--min-interval-ms", type=float, default=10.0)
    peer.add_argument("--max-interval-ms", type=float, default=100.0)
    _add_fault_options(peer)
    peer.set_defaults(handler=_run_peer)

    coordinator = commands.add_parser("coordinator", help="run the comparison server")
    _add_group_manager_option(coordinator)
    _add_host_options(coordinator)
    coordinator.add_argument(
        "--port", type=int, default=int(_env("COORDINATOR_PORT", str(DEFAULT_COORDINATOR_PORT)))
    )
    coordinator.add_argument("--peers", type=int, default=int(_env("PEERS", "6")))
    coordinator.add_argument(
        "--messages", type=int, help="messages per peer; runs non-interactively when given"
    )
    coordinator.add_argument("--rounds", type=int, default=1)
    coordinator.add_argument("--report", type=Path, help="write the round reports as JSON")
    coordinator.add_argument("--round-timeout", type=float, default=180.0)
    coordinator.add_argument("--startup-timeout", type=float, default=60.0)
    coordinator.add_argument(
        "--keep-group", action="store_true", help="do not stop peers and group manager at the end"
    )
    coordinator.set_defaults(handler=_run_coordinator)

    local = commands.add_parser("local", help="run a complete experiment on localhost")
    local.add_argument("--peers", type=int, default=4)
    local.add_argument("--messages", type=int, default=20)
    local.add_argument("--rounds", type=int, default=1)
    local.add_argument("--base-port", type=int, default=7000)
    local.add_argument("--report", type=Path)
    local.add_argument("--timeout", type=float, default=300.0)
    _add_fault_options(local)
    local.set_defaults(handler=_run_local)
    return parser


def _run_group_manager(args: argparse.Namespace) -> int:
    run_group_manager(args.listen)
    return 0


def _run_peer(args: argparse.Namespace) -> int:
    settings = PeerSettings(
        group_manager=args.group_manager,
        bind_host=args.bind,
        advertise_host=args.advertise_host,
        udp_port=args.udp_port,
        control_port=args.control_port,
        faults=_fault_profile(args),
        timing=RoundTiming(
            min_interval=args.min_interval_ms / 1000, max_interval=args.max_interval_ms / 1000
        ),
    )
    PeerNode(settings).run()
    return 0


def _run_coordinator(args: argparse.Namespace) -> int:
    settings = CoordinatorSettings(
        listen=Endpoint(args.bind, args.port),
        advertise_host=args.advertise_host,
        group_manager=args.group_manager,
        expected_peers=args.peers,
        startup_timeout=args.startup_timeout,
        round_timeout=args.round_timeout,
    )
    if args.messages is None:
        return run_interactive(settings)
    if args.messages <= 0 or args.rounds <= 0:
        raise ValueError("--messages and --rounds must be positive")
    return run_batch(settings, args.messages, args.rounds, args.report, args.keep_group)


def _run_local(args: argparse.Namespace) -> int:
    experiment = LocalExperiment(
        peers=args.peers,
        messages=args.messages,
        rounds=args.rounds,
        base_port=args.base_port,
        faults=_fault_profile(args),
        report=args.report,
        timeout=args.timeout,
        log_level=args.log_level,
    )
    return run_local(experiment)


def _add_group_manager_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--group-manager",
        type=Endpoint.parse,
        default=_env("GROUP_MANAGER", DEFAULT_GROUP_MANAGER),
        help="host:port of the group manager",
    )


def _add_host_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bind", default=_env("BIND", "0.0.0.0"), help="local interface to bind")
    parser.add_argument(
        "--advertise-host",
        default=_env("ADVERTISE_HOST", _detect_host()),
        help="address other processes use to reach this one",
    )


def _add_fault_options(parser: argparse.ArgumentParser) -> None:
    faults = parser.add_argument_group("fault injection")
    faults.add_argument("--loss", type=float, default=float(_env("LOSS", "0")))
    faults.add_argument("--duplication", type=float, default=float(_env("DUPLICATION", "0")))
    faults.add_argument("--delay-ms", type=float, default=float(_env("DELAY_MS", "0")))
    faults.add_argument("--jitter-ms", type=float, default=float(_env("JITTER_MS", "0")))
    faults.add_argument("--seed", type=int, default=None)


def _fault_profile(args: argparse.Namespace) -> FaultProfile:
    return FaultProfile(
        loss=args.loss,
        duplication=args.duplication,
        delay_ms=args.delay_ms,
        jitter_ms=args.jitter_ms,
        seed=args.seed,
    )


def _env(name: str, default: str) -> str:
    return os.environ.get(f"MPCOMM_{name}", default)


def _detect_host() -> str:
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


if __name__ == "__main__":
    sys.exit(main())
