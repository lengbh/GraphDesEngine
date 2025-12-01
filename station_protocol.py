"""
Binary protocol helpers for station messages, converted from C structs.

C definitions:

#define MSG_STATION_ACTION_QUERY           0x1046
#define MSG_STATION_ACTION_DONE_QUERY      0x1047

typedef struct
{
    uint32_t        workstation_id;
    uint32_t        tray_id;
} ST_StationActionQuery;

#define MSG_STATION_ACTION_RSP             0x1048

typedef struct
{
    ST_StationActionQuery qry;
    uint32_t    order_id;
    uint32_t    action_type; // 0 for release and read next_station_id, 1 for execute and ignore next_station_id
    uint32_t    next_station_id;
} ST_StationActionRsp;

Assumptions:
- Little-endian layout for all 32-bit integers (common on x86/x64). If your peer uses
  big-endian/network order, switch '<' to '>' in the struct format strings.
- No padding between fields beyond natural 4-byte alignment for the given layout.
"""

from __future__ import annotations
import struct
from dataclasses import dataclass
from typing import ClassVar

# Message type constants
MSG_STATION_ACTION_QUERY = 0x1046
MSG_STATION_ACTION_DONE_QUERY = 0x1047
MSG_STATION_ACTION_RSP  = 0x1048

# Struct format strings (little-endian unsigned 32-bit)
# On-the-wire order: workstation_id first, then tray_id
_FMT_U32_2 = '<II'   # workstation_id, tray_id
_FMT_U32_3 = '<III'  # workstation_id, tray_id, extra

# Bring in TCPMsg from the Python wrapper of the pybind11 module
from tcp_client import TCPMsg


def _make_msg(msg_type: int, payload: bytes) -> TCPMsg:
    """Create a TCPMsg with given type and payload bytes."""
    m = TCPMsg()
    m.header.type = int(msg_type)
    m.body = list(payload)
    return m


@dataclass(frozen=True)
class StationActionQuery:
    workstation_id: int
    tray_id: int

    FMT: ClassVar[str] = _FMT_U32_2
    SIZE: ClassVar[int] = struct.calcsize(FMT)

    def pack(self) -> TCPMsg:
        payload = struct.pack(self.FMT, self.workstation_id & 0xFFFFFFFF, self.tray_id & 0xFFFFFFFF)
        return _make_msg(MSG_STATION_ACTION_QUERY, payload)

    @staticmethod
    def unpack(b: bytes) -> 'StationActionQuery':
        workstation_id, tray_id = struct.unpack(StationActionQuery.FMT, b[:StationActionQuery.SIZE])
        return StationActionQuery(workstation_id=workstation_id, tray_id=tray_id)


@dataclass(frozen=True)
class StationActionDoneQuery:
    workstation_id: int
    tray_id: int

    FMT: ClassVar[str] = _FMT_U32_2
    SIZE: ClassVar[int] = struct.calcsize(FMT)

    def pack(self) -> TCPMsg:
        payload = struct.pack(self.FMT, self.workstation_id & 0xFFFFFFFF, self.tray_id & 0xFFFFFFFF)
        return _make_msg(MSG_STATION_ACTION_DONE_QUERY, payload)

    @staticmethod
    def unpack(b: bytes) -> 'StationActionDoneQuery':
        workstation_id, tray_id = struct.unpack(StationActionDoneQuery.FMT, b[:StationActionDoneQuery.SIZE])
        return StationActionDoneQuery(workstation_id=workstation_id, tray_id=tray_id)


@dataclass(frozen=True)
class StationActionRsp:
    qry: StationActionQuery
    order_id: int
    action_type: int
    next_station_id: int

    # Layout: workstation_id, tray_id, order_id, action_type, next_station_id
    FMT: ClassVar[str] = '<IIIII'
    SIZE: ClassVar[int] = struct.calcsize(FMT)

    def pack(self, msg_type: int = MSG_STATION_ACTION_RSP) -> TCPMsg:
        payload = struct.pack(
            self.FMT,
            self.qry.workstation_id & 0xFFFFFFFF,
            self.qry.tray_id & 0xFFFFFFFF,
            self.order_id & 0xFFFFFFFF,
            self.action_type & 0xFFFFFFFF,
            self.next_station_id & 0xFFFFFFFF,
        )
        return _make_msg(msg_type, payload)

    @staticmethod
    def unpack(b: bytes) -> 'StationActionRsp':
        workstation_id, tray_id, order_id, action_type, next_station_id = struct.unpack(StationActionRsp.FMT, b[:StationActionRsp.SIZE])
        return StationActionRsp(StationActionQuery(workstation_id, tray_id), order_id, action_type, next_station_id)
