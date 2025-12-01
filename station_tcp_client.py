import queue
import simpy
from simpy.events import Event as SimEvent
from tcp_client import TCPClient, TCPMsg

from station_protocol import (
    StationActionQuery,
    StationActionDoneQuery,
    StationActionRsp,
    MSG_STATION_ACTION_QUERY,
    MSG_STATION_ACTION_DONE_QUERY,
    MSG_STATION_ACTION_RSP,
)


class StationTCPClient(TCPClient):
    """
    TCP connection with MES asynchronous responses using `PyTCPClient`.

    Protocol:
    - Requests on arrival or on finishing service:
      - MSG_STATION_ACTION_QUERY (0x1046): body = StationActionQuery(tray_id, workstation_id)
      - MSG_STATION_ACTION_DONE_QUERY (0x1047): body = StationActionDoneQuery(tray_id, workstation_id)
    - Unified Response:
      - MSG_STATION_ACTION_RSP (0x1048): body = StationActionRsp(qry, action_type, next_station_id)
        action_type: 0 = release (use next_station_id), 1 = execute (ignore next_station_id)

    """

    def __init__(self, env: simpy.Environment, host: str, port: int, timeout: float = 2.0):
        super().__init__()
        self.env = env
        self.host = host
        self.port = port
        self.timeout = max(0.0, float(timeout))

        self._recv_q: queue.Queue[tuple[str, tuple[int, int], int | None]] = queue.Queue()
        self._pending_route: dict[tuple[int, int], SimEvent] = {}
        self._pending_action: dict[tuple[int, int], SimEvent] = {}

        # Establish connection and start background network loop
        if not self.connect_and_wait(self.host, self.port, timeout=self.timeout):
            print("[TcpAgent] Failed to connect; will continue and rely on later reconnect attempts")
        self.run_background()

        # Start a SimPy pump to complete events inside the SimPy thread
        self.env.process(self._pump())

    def close(self):
        self.stop()

    def send_station_action_query(self, tray_id: int, workstation_id: int):
        qry = StationActionQuery(tray_id=tray_id, workstation_id=workstation_id)
        msg = qry.pack()
        self.send_msg(msg)

    def send_station_action_done_qry(self, tray_id: int, workstation_id: int):
        qry = StationActionDoneQuery(tray_id=tray_id, workstation_id=workstation_id)
        msg = qry.pack()
        self.send_msg(msg)

    def request_action(self, workstation_id: int, tray_id: int) -> tuple[int, int, int] | tuple[None, None, None]:
        """Send StationActionQuery and synchronously wait for StationActionRsp.
        Returns (action_type, next_station_id), or (None, None) on timeout/error.
        Note: This blocks the calling thread until a response arrives or timeout.
        """
        try:
            self.send_station_action_query(tray_id, workstation_id)
        except Exception:
            return None, None, None

        def _pred(msg: TCPMsg) -> bool:
            try:
                if getattr(msg.header, 'type', None) != MSG_STATION_ACTION_RSP:
                    return False
                body = bytes(msg.body)
                rsp = StationActionRsp.unpack(body)
                return int(rsp.qry.tray_id) == int(tray_id) and int(rsp.qry.workstation_id) == int(workstation_id)
            except Exception:
                return False

        m = self.wait_for_message(timeout=self.timeout, predicate=_pred)
        if m is None:
            return None, None, None
        try:
            rsp = StationActionRsp.unpack(bytes(m.body))
            return int(rsp.order_id), int(rsp.action_type), int(rsp.next_station_id)
        except Exception:
            return None, None, None

    def request_action_async(self, workstation_id: int, tray_id: int) -> SimEvent:
        """Non-blocking version: returns a SimPy Event that will be resolved with action_type (int) or None."""
        key = (int(tray_id), int(workstation_id))
        ev = SimEvent(self.env)
        self._pending_action[key] = ev
        # Optional timeout metadata for callers that support it
        setattr(ev, '_decide_timeout', self.timeout)
        setattr(ev, '_default_next', None)
        try:
            self.send_station_action_query(tray_id, workstation_id)
        except Exception:
            if not ev.triggered:
                ev.succeed(None)
        return ev

    def request_routing(self, workstation_id: int, tray_id: int) -> tuple[int, int, int] | tuple[None, None, None]:
        """Send StationActionDoneQuery and synchronously wait for StationActionRsp.
        Returns (action_type, next_station_id) or (None, None) on timeout/error.
        Note: This blocks the calling thread until a response arrives or timeout.
        """
        try:
            self.send_station_action_done_qry(tray_id, workstation_id)
        except Exception:
            return None, None, None

        def _pred(msg: TCPMsg) -> bool:
            try:
                if getattr(msg.header, 'type', None) != MSG_STATION_ACTION_RSP:
                    return False
                body = bytes(msg.body)
                rsp = StationActionRsp.unpack(body)
                return int(rsp.qry.tray_id) == int(tray_id) and int(rsp.qry.workstation_id) == int(workstation_id)
            except Exception:
                return False

        m = self.wait_for_message(timeout=self.timeout, predicate=_pred)
        if m is None:
            return None, None, None
        try:
            rsp = StationActionRsp.unpack(bytes(m.body))
            return int(rsp.order_id), int(rsp.action_type), int(rsp.next_station_id)
        except Exception:
            return None, None, None

    def request_routing_async(self, workstation_id: int, tray_id: int) -> SimEvent:
        """Non-blocking version: returns a SimPy Event that will be resolved with next_station (int) or None."""
        key = (int(tray_id), int(workstation_id))
        ev = SimEvent(self.env)
        self._pending_route[key] = ev
        # Optional timeout metadata for callers that support it
        setattr(ev, '_decide_timeout', self.timeout)
        setattr(ev, '_default_next', None)
        try:
            self.send_station_action_done_qry(tray_id, workstation_id)
        except Exception:
            if not ev.triggered:
                ev.succeed(None)
        return ev

    # -------- PyTCPClient hooks --------
    def on_message(self, msg):  # msg is tcp.TCPMsg
        try:
            mtype = getattr(msg.header, 'type', None)
            if mtype == MSG_STATION_ACTION_RSP:
                rsp = StationActionRsp.unpack(bytes(msg.body))
                key = (int(rsp.qry.tray_id), int(rsp.qry.workstation_id))
                self._recv_q.put(("action", key, int(rsp.action_type)))
                if int(rsp.action_type) == 0:
                    self._recv_q.put(("route", key, int(rsp.next_station_id)))
            else:
                # Unknown message type; ignore
                pass
        except Exception as e:
            print(f"[TcpAgent] Failed to parse incoming message: {e}")

    # ---- SimPy pump ----
    def _pump(self):
        while True:
            # Drain receive queue and succeed matching events
            while True:
                try:
                    kind, key, value = self._recv_q.get_nowait()
                except queue.Empty:
                    break
                if kind == "route":
                    ev = self._pending_route.pop(key, None)
                elif kind == "action":
                    ev = self._pending_action.pop(key, None)
                else:
                    ev = None
                if ev is not None and not ev.triggered:
                    ev.succeed(value)
            # Advance simulation time minimally to yield control
            yield self.env.timeout(0.01)


