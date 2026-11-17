from mpcomm.model import Peer, PeerStats, RoundLog
from mpcomm.rpc import mpcomm_pb2 as pb


def peer_to_proto(peer: Peer) -> pb.PeerInfo:
    return pb.PeerInfo(
        peer_id=peer.peer_id,
        host=peer.host,
        udp_port=peer.udp_port,
        control_port=peer.control_port,
    )


def peer_from_proto(message: pb.PeerInfo) -> Peer:
    return Peer(message.peer_id, message.host, message.udp_port, message.control_port)


def round_log_to_proto(log: RoundLog) -> pb.RoundLog:
    stats = log.stats
    return pb.RoundLog(
        round_id=log.round_id,
        peer_id=log.peer_id,
        completed=log.completed,
        delivered=[
            pb.DeliveredMessage(sender=sender, number=number) for sender, number in log.delivered
        ],
        stats=pb.PeerStats(
            data_sent=stats.data_sent,
            retransmissions=stats.retransmissions,
            acks_sent=stats.acks_sent,
            duplicates_discarded=stats.duplicates_discarded,
            held_back=stats.held_back,
            dropped_by_fault_injector=stats.dropped_by_fault_injector,
            unreachable_peers=list(stats.unreachable_peers),
        ),
    )


def round_log_from_proto(message: pb.RoundLog) -> RoundLog:
    stats = message.stats
    return RoundLog(
        round_id=message.round_id,
        peer_id=message.peer_id,
        completed=message.completed,
        delivered=tuple((item.sender, item.number) for item in message.delivered),
        stats=PeerStats(
            data_sent=stats.data_sent,
            retransmissions=stats.retransmissions,
            acks_sent=stats.acks_sent,
            duplicates_discarded=stats.duplicates_discarded,
            held_back=stats.held_back,
            dropped_by_fault_injector=stats.dropped_by_fault_injector,
            unreachable_peers=tuple(stats.unreachable_peers),
        ),
    )
