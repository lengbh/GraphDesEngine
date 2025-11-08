import time
import threading
from typing import Optional, Callable, Any

try:
    from tcpconn_py import TCPClientMsg, TCPMsg
except ImportError as e:
    TCPClientMsg = Any
    TCPMsg = Any
    print(
        "Failed to import tcpconn_py. Make sure you've built the 'tcpconn_py' target and that the directory "
        "containing the module is on PYTHONPATH, and dependency DLL directories are added if on Windows.\n"
        f"Error: {e}"
    )
    raise

__all__ = ['TCPClient', 'TCPMsg']

class TCPClient(TCPClientMsg):
    """
    A reusable Python base client for framed TCP messages (`TCPMsg`).

    Child classes can override these hooks:
      - on_connected(self)
      - on_disconnected(self)
      - on_message(self, msg: TCPMsg)

    Utilities:
      - connect_and_wait(host, port, timeout)
      - run_background() / stop()
      - send_msg(msg), send_text(text, type), send_bytes(b, type)
      - wait_for_message(timeout, predicate)
      - pump_until(timeout, predicate, wait=False)
    """

    def __init__(self):
        super().__init__()
        self.connected_event = threading.Event()
        self.disconnected_event = threading.Event()
        self.message_event = threading.Event()
        self.last_msg: Optional[TCPMsg] = None
        self._runner: Optional[threading.Thread] = None

    # -------- C++ callback overrides: translate to Python hooks + signal events --------
    def OnConnected(self):
        print("[PY] Connected to server (validated)")
        self.disconnected_event.clear()
        self.connected_event.set()
        self.on_connected()

    def OnDisconnected(self):
        print("[PY] Disconnected from server")
        self.connected_event.clear()
        self.disconnected_event.set()
        self.on_disconnected()

    def OnMessage(self, msg: TCPMsg):
        self.last_msg = msg
        self.message_event.set()
        self.on_message(msg)

    # -------- Python-level hooks for child classes --------
    def on_connected(self):
        """Hook: called after connection is validated."""
        pass

    def on_disconnected(self):
        """Hook: called on disconnection."""
        pass

    def on_message(self, msg: TCPMsg):
        """Hook: override to handle messages."""
        print("[PY] Received message:")
        print(msg.formatted())

    # Helper methods
    def connect_and_wait(self, host: str, port: int, timeout: float = 5.0) -> bool:
        if not self.connect(host, port):
            return False
        if not self.connected_event.wait(timeout=timeout):
            print("[PY] Timed out waiting for validated connection ❌")
            self.disconnect()
            return False
        return True

    @staticmethod
    def _ensure_header_size(m: TCPMsg) -> None:
        # Ensure header.size matches header + body length
        m.header.size = m.full_size()

    def send_msg(self, m: TCPMsg) -> None:
        self._ensure_header_size(m)
        self.send(m)

    # text and byte sender
    def send_text(self, text: str, type: int = 1, encoding: str = "utf-8") -> TCPMsg:
        b = text.encode(encoding)
        return self.send_bytes(b, type)

    def send_bytes(self, b: bytes, type: int = 1) -> TCPMsg:
        m = TCPMsg()
        m.header.type = type
        m.body = list(b)
        self.send_msg(m)
        return m

    # A runner function is required
    def run_background(self) -> None:
        if self._runner and self._runner.is_alive():
            return

        def _target():
            try:
                self.run()  # blocks; releases GIL in binding
            except Exception as e:
                print(f"[PY] Background run() exited with error: {e}")

        self._runner = threading.Thread(target=_target, daemon=True)
        self._runner.start()

    def stop(self, join_timeout: float = 1.0) -> None:
        self.disconnect()
        if self._runner:
            self._runner.join(timeout=join_timeout)

    def wait_for_message(
        self,
        timeout: float,
        predicate: Optional[Callable[[TCPMsg], bool]] = None,
        sleep: float = 0.02,
    ) -> Optional[TCPMsg]:
        """
        Wait (with periodic non-blocking updates) until a message arrives that satisfies `predicate`.
        If predicate is None, the first incoming message will satisfy the wait.
        Returns the matching message, or None on timeout.
        """
        self.message_event.clear()
        deadline = time.time() + timeout
        while time.time() < deadline:
            # pump incoming without blocking to maintain timeout control
            self.update(False)
            if self.message_event.is_set() and self.last_msg is not None:
                if predicate is None or predicate(self.last_msg):
                    return self.last_msg
                # Not matched; keep waiting for another message
                self.message_event.clear()
            time.sleep(sleep)
        return None

    def pump_until(
        self,
        timeout: float,
        predicate: Callable[[], bool],
        wait: bool = False,
        sleep: float = 0.02,
    ) -> bool:
        """Pump `update()` until predicate() is True or timeout. Returns True if satisfied."""
        deadline = time.time() + timeout
        while time.time() < deadline and not predicate():
            self.update(wait)
            if not wait:
                time.sleep(sleep)
        return predicate()


# ---------------- Example subclass and CLI demo ----------------
class EchoClient(TCPClient):
    def on_message(self, msg: TCPMsg):
        print("[PY] Echo received:")
        print(msg.formatted())


def example():
    client = EchoClient()

    if not client.connect_and_wait("192.168.0.201", 6789, timeout=5.0):
        print(f"[PY] Failed to connect server")
        return 2

    msg = client.send_text("Hello from python", type=123)
    print(f"[PY] Sending {len(msg.body)} bytes, type={msg.header.type}")

    try:
        echoed = client.wait_for_message(timeout=5.0)
    except KeyboardInterrupt:
        print("[PY] Interrupted by user")
        echoed = None
    finally:
        client.disconnect()

    if echoed is not None:
        print("[PY] Echo received ✅")
        return 0
    else:
        print("[PY] No echo received within timeout ❌")
        return 1


if __name__ == "__main__":
    raise SystemExit(example())
