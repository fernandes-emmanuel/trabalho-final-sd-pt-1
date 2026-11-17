from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PeerInfo(_message.Message):
    __slots__ = ("peer_id", "host", "udp_port", "control_port")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    UDP_PORT_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PORT_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    host: str
    udp_port: int
    control_port: int
    def __init__(self, peer_id: _Optional[int] = ..., host: _Optional[str] = ..., udp_port: _Optional[int] = ..., control_port: _Optional[int] = ...) -> None: ...

class RegisterRequest(_message.Message):
    __slots__ = ("host", "udp_port", "control_port")
    HOST_FIELD_NUMBER: _ClassVar[int]
    UDP_PORT_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PORT_FIELD_NUMBER: _ClassVar[int]
    host: str
    udp_port: int
    control_port: int
    def __init__(self, host: _Optional[str] = ..., udp_port: _Optional[int] = ..., control_port: _Optional[int] = ...) -> None: ...

class RegisterReply(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    def __init__(self, peer_id: _Optional[int] = ...) -> None: ...

class UnregisterRequest(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    def __init__(self, peer_id: _Optional[int] = ...) -> None: ...

class PeerList(_message.Message):
    __slots__ = ("peers",)
    PEERS_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[PeerInfo]
    def __init__(self, peers: _Optional[_Iterable[_Union[PeerInfo, _Mapping]]] = ...) -> None: ...

class StartRequest(_message.Message):
    __slots__ = ("round_id", "messages_per_peer", "report_to")
    ROUND_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_PER_PEER_FIELD_NUMBER: _ClassVar[int]
    REPORT_TO_FIELD_NUMBER: _ClassVar[int]
    round_id: int
    messages_per_peer: int
    report_to: str
    def __init__(self, round_id: _Optional[int] = ..., messages_per_peer: _Optional[int] = ..., report_to: _Optional[str] = ...) -> None: ...

class StartReply(_message.Message):
    __slots__ = ("peer_id", "group_size")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_SIZE_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    group_size: int
    def __init__(self, peer_id: _Optional[int] = ..., group_size: _Optional[int] = ...) -> None: ...

class DeliveredMessage(_message.Message):
    __slots__ = ("sender", "number")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    sender: int
    number: int
    def __init__(self, sender: _Optional[int] = ..., number: _Optional[int] = ...) -> None: ...

class PeerStats(_message.Message):
    __slots__ = ("data_sent", "retransmissions", "acks_sent", "duplicates_discarded", "held_back", "dropped_by_fault_injector", "unreachable_peers")
    DATA_SENT_FIELD_NUMBER: _ClassVar[int]
    RETRANSMISSIONS_FIELD_NUMBER: _ClassVar[int]
    ACKS_SENT_FIELD_NUMBER: _ClassVar[int]
    DUPLICATES_DISCARDED_FIELD_NUMBER: _ClassVar[int]
    HELD_BACK_FIELD_NUMBER: _ClassVar[int]
    DROPPED_BY_FAULT_INJECTOR_FIELD_NUMBER: _ClassVar[int]
    UNREACHABLE_PEERS_FIELD_NUMBER: _ClassVar[int]
    data_sent: int
    retransmissions: int
    acks_sent: int
    duplicates_discarded: int
    held_back: int
    dropped_by_fault_injector: int
    unreachable_peers: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, data_sent: _Optional[int] = ..., retransmissions: _Optional[int] = ..., acks_sent: _Optional[int] = ..., duplicates_discarded: _Optional[int] = ..., held_back: _Optional[int] = ..., dropped_by_fault_injector: _Optional[int] = ..., unreachable_peers: _Optional[_Iterable[int]] = ...) -> None: ...

class RoundLog(_message.Message):
    __slots__ = ("round_id", "peer_id", "completed", "delivered", "stats")
    ROUND_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    round_id: int
    peer_id: int
    completed: bool
    delivered: _containers.RepeatedCompositeFieldContainer[DeliveredMessage]
    stats: PeerStats
    def __init__(self, round_id: _Optional[int] = ..., peer_id: _Optional[int] = ..., completed: _Optional[bool] = ..., delivered: _Optional[_Iterable[_Union[DeliveredMessage, _Mapping]]] = ..., stats: _Optional[_Union[PeerStats, _Mapping]] = ...) -> None: ...
