import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Callable

import simpy
from simpy.events import Event

from labelled_graph import Vertex, Arc, LabelledGraph
from random_util import RandomFactory

if TYPE_CHECKING:
    from station_tcp_client import StationTCPClient

DEFAULT_GRAPH_MODEL_FILE = 'SystemGraphs/two_station_circular_system_graph.json'
DEFAULT_OUT_LOG_CSV_FILE = 'EventLogs/event_logs.csv'

CSV_FIELDS = [
    'time',
    'station_id',
    'tray_id',
    'workpiece_id',
    'activity',
]


@dataclass
class Workpiece:
    id: int
    created_at: float
    completed_at: float


@dataclass
class Tray:
    id: int
    current_workpiece_id: int = 0xFFFFFFFF


class VertexRuntime:
    """Runtime state of one station.

    The station has:
    - an input buffer (`buffer`)
    - a free-slot counter for that buffer (`free_slots`)
    - a service resource (`server`)

    Under BAS (blocking-after-service), a finished tray may not be released
    until the downstream station has a reserved input-buffer slot.

    In MES mode, the simulator does not enforce that BAS policy. It only
    executes the MES action/routing responses and leaves the blocking policy
    decision to the MES layer.
    """

    def __init__(self,
                 env: simpy.Environment,
                 vertex: Vertex,
                 get_arc_to_next_vertex: Callable[[int, int], Arc],
                 select_next_vertex: Callable[[int], int],
                 reserve_buffer_slot: Callable[[int], Event],
                 release_buffer_slot: Callable[[int], Event],
                 transfer_to_next_vertex: Callable[[int, Tray, bool], Event],
                 emit_event: Callable[[dict], None],
                 client: 'StationTCPClient | None' = None):
        self.env = env
        self.vertex = vertex
        self.mes_control_mode = client is not None

        if client is None:
            # Local mode: always process at the station, then route using the graph.
            # TODO in serial mode the order id can be the tray id
            # TODO in mes less non serial mode, the order id should be increasingly assigned
            self.request_action = lambda _, __: (0, 1, 0)
            self.request_next_vertex = lambda current_vertex, __: (0, 0, select_next_vertex(current_vertex))
        else:
            # MES connected mode, request from MES for both action and next vertex after service.
            self.request_action = client.request_action
            self.request_next_vertex = client.request_routing

        self.get_arc_to_next_vertex = get_arc_to_next_vertex
        self.buffer = simpy.Store(env, capacity=vertex.buffer_capacity)
        # `free_slots` is the authoritative capacity tracker used by BAS.
        # A slot is reserved before release, then consumed by the actual enqueue.
        self.free_slots = simpy.Container(env, capacity=vertex.buffer_capacity, init=vertex.buffer_capacity)
        self.server = simpy.Resource(env, capacity=1)
        self.emit_event = emit_event
        self.reserve_buffer_slot = reserve_buffer_slot
        self.release_buffer_slot = release_buffer_slot
        self.transfer_to_next_vertex = transfer_to_next_vertex

        self.worker_process = env.process(self._worker())

    def _worker(self):
        while True:
            # Removing a tray from the input buffer frees one slot immediately,
            # even if the tray is still waiting for service or later blocked by BAS.
            tray = yield self.buffer.get()
            yield self.release_buffer_slot(self.vertex.id)
            self.emit_event({
                'type': 'dequeued',
                't': self.env.now,
                'vertex_id': self.vertex.id,
                'tray_id': tray.id,
                'workpiece_id': tray.current_workpiece_id,
            })

            order_id, action, next_v = self.request_action(self.vertex.id, tray.id)
            tray.current_workpiece_id = order_id

            # TODO add error and exception handling around client interaction
            if action == 0:
                yield self.env.process(self._dispatch_to_next_vertex(next_v, tray))
                continue

            yield self.env.process(self._service_tray(tray))

            if self.vertex.is_sink:
                # Serial-system sink: a finished tray leaves the system here instead of
                # requesting routing and re-entering the graph.
                self._complete_tray(tray)
                continue

            order_id, action, next_v = self.request_next_vertex(self.vertex.id, tray.id)
            tray.current_workpiece_id = order_id
            assert action == 0
            yield self.env.process(self._dispatch_to_next_vertex(next_v, tray))

    def _service_tray(self, tray: Tray):
        with self.server.request() as req:
            yield req
            service_time = self.vertex.service()
            self.emit_event({
                'type': 'service_start',
                't': self.env.now,
                'vertex_id': self.vertex.id,
                'tray_id': tray.id,
                'workpiece_id': tray.current_workpiece_id,
                'service_time': service_time,
            })
            yield self.env.timeout(service_time)
            self.emit_event({
                'type': 'service_end',
                't': self.env.now,
                'vertex_id': self.vertex.id,
                'tray_id': tray.id,
                'workpiece_id': tray.current_workpiece_id,
            })

    def _complete_tray(self, tray: Tray):
        self.emit_event({
            'type': 'tray_completed',
            't': self.env.now,
            'vertex_id': self.vertex.id,
            'tray_id': tray.id,
            'workpiece_id': tray.current_workpiece_id,
        })

    def _release_to_next_vertex(self, next_vertex: int, tray: Tray):
        """Reserve downstream capacity before release, then start transport asynchronously.

        Under BAS, the station is blocked only until downstream capacity is available.
        Once the slot is reserved, the tray is released and physical transport proceeds
        in a separate process while this worker can dequeue the next tray.
        """
        arc_to_next_vertex = self.get_arc_to_next_vertex(self.vertex.id, next_vertex)
        transfer_time = arc_to_next_vertex.transfer()
        yield self.reserve_buffer_slot(next_vertex)
        self.env.process(self._transfer_process(next_vertex, tray, transfer_time, slot_reserved=True))

    def _dispatch_to_next_vertex(self, next_vertex: int, tray: Tray):
        """Route a tray to the next vertex under the active control mode."""
        if self.mes_control_mode:
            yield self.env.process(self._transfer_with_mes_policy(next_vertex, tray))
            return

        yield self.env.process(self._release_to_next_vertex(next_vertex, tray))

    def _transfer_with_mes_policy(self, next_vertex: int, tray: Tray):
        """MES-controlled routing path without simulator-side BAS reservation.

        MES decides the blocking/routing policy. The simulator starts the
        transfer immediately and does not block the station worker on the
        physical transfer duration.
        """
        arc_to_next_vertex = self.get_arc_to_next_vertex(self.vertex.id, next_vertex)
        transfer_time = arc_to_next_vertex.transfer()
        self.env.process(self._transfer_process(next_vertex, tray, transfer_time, slot_reserved=False))
        yield self.env.timeout(0)

    def _transfer_process(self, next_vertex: int, tray: Tray, transfer_time: float, slot_reserved: bool):
        self.emit_event({
            'type': 'transfer_start',
            't': self.env.now,
            'tray_id': tray.id,
            'workpiece_id': tray.current_workpiece_id,
            'tail': self.vertex.id,
            'head': next_vertex,
            'transfer_time': transfer_time,
        })
        yield self.env.timeout(transfer_time)
        yield self.transfer_to_next_vertex(next_vertex, tray, slot_reserved=slot_reserved)
        self.emit_event({
            'type': 'transfer_end',
            't': self.env.now,
            'tray_id': tray.id,
            'workpiece_id': tray.current_workpiece_id,
            'tail': self.vertex.id,
            'head': next_vertex,
        })


class GraphSimulation:
    """Simulation wrapper around the graph runtime and event logging."""

    def __init__(self, graph: LabelledGraph, env: simpy.Environment, out_log_csv_file: str,
                 mes_control_mode: bool, mes_host: str = None, mes_port: int = None,
                 event_listeners: list[Callable[[dict], None]] | None = None):
        self.graph = graph
        self.env = env
        self.out_log_csv_file = out_log_csv_file
        self.vertices: dict[int, VertexRuntime] = {}
        self._listeners: list[Callable[[dict], None]] = list(event_listeners or [])
        self._next_tray_id = 1
        self._trays: dict[int, Tray] = {}
        self._completed: list[int] = []

        self._create_vertex_runtimes(mes_control_mode, mes_host, mes_port)

        self.env.process(self._completion_monitor())

    def add_event_listener(self, listener: Callable[[dict], None]):
        self._listeners.append(listener)

    def get_source_vertex_ids(self) -> list[int]:
        source_vertex_ids = sorted(vertex.id for vertex in self.graph.vertices.values() if vertex.is_source)
        return source_vertex_ids or [1]

    def _create_tray(self) -> Tray:
        tray_id = self._next_tray_id
        self._next_tray_id += 1
        tray = Tray(id=tray_id, current_workpiece_id=0)
        self._trays[tray_id] = tray
        return tray

    def _create_vertex_runtimes(self, mes_control_mode: bool, mes_host: str | None, mes_port: int | None):
        if not mes_control_mode:
            for vertex in self.graph.vertices.values():
                self.vertices[vertex.id] = self._build_vertex_runtime(vertex, client=None)
            return

        from station_tcp_client import StationTCPClient

        self._clients: dict[int, StationTCPClient] = {}
        for vertex in self.graph.vertices.values():
            client = StationTCPClient(env=self.env, host=mes_host, port=mes_port, timeout=60)
            self._clients[vertex.id] = client
            self.vertices[vertex.id] = self._build_vertex_runtime(vertex, client=client)

    def _build_vertex_runtime(self, vertex: Vertex, client: 'StationTCPClient | None') -> VertexRuntime:
        return VertexRuntime(
            self.env,
            vertex,
            get_arc_to_next_vertex=self.graph.get_arc,
            select_next_vertex=self.graph.select_next_vertex,
            reserve_buffer_slot=self.reserve_buffer_slot,
            release_buffer_slot=self.release_buffer_slot,
            transfer_to_next_vertex=self.transfer_to_next_vertex,
            emit_event=self._emit,
            client=client,
        )

    def _completion_monitor(self):
        while True:
            yield self.env.timeout(1.0)
            # TODO check ending conditions using listeners or MES messages
            pass

    def inject_tray(self, spawn_vertex_id: int, at: float = 0) -> int:
        tray = self._create_tray()
        tray_id = tray.id

        def _spawn():
            if at > 0:
                yield self.env.timeout(at)

            vertex = self.vertices.get(spawn_vertex_id)
            if vertex is None:
                # A missing spawn vertex is treated as immediate completion.
                self._completed.append(tray_id)
                self._emit({
                    'type': 'tray_completed',
                    't': self.env.now,
                    'vertex_id': spawn_vertex_id,
                    'tray_id': tray_id,
                    'workpiece_id': tray.current_workpiece_id,
                })
                return

            self._emit({
                'type': 'injected',
                't': self.env.now,
                'vertex_id': spawn_vertex_id,
                'tray_id': tray_id,
                'workpiece_id': tray.current_workpiece_id,
            })
            yield self.transfer_to_next_vertex(spawn_vertex_id, tray)

        self.env.process(_spawn())
        return tray_id

    def start_serial_injection(self):
        for source_vertex_id in self.get_source_vertex_ids():
            self.env.process(self._source_feeder(source_vertex_id))

    def _source_feeder(self, source_vertex_id: int):
        # Serial mode keeps source buffers fed continuously. A new tray is
        # created whenever one source-buffer slot becomes available.
        while True:
            yield self.reserve_buffer_slot(source_vertex_id)
            tray = self._create_tray()
            self._emit({
                'type': 'injected',
                't': self.env.now,
                'vertex_id': source_vertex_id,
                'tray_id': tray.id,
                'workpiece_id': tray.current_workpiece_id,
            })
            yield self.transfer_to_next_vertex(source_vertex_id, tray, slot_reserved=True)

    def reserve_buffer_slot(self, vertex_id: int) -> Event:
        """Reserve one downstream input-buffer slot before a BAS release."""
        vertex = self.vertices.get(vertex_id)
        return vertex.free_slots.get(1)

    def release_buffer_slot(self, vertex_id: int) -> Event:
        """Return one input-buffer slot after a tray leaves a station buffer."""
        vertex = self.vertices.get(vertex_id)
        return vertex.free_slots.put(1)

    def transfer_to_next_vertex(self, vertex_id: int, tray: Tray, slot_reserved: bool = False) -> Event:
        """Enqueue a tray into a station buffer.

        If `slot_reserved` is False, this method reserves capacity itself.
        If `slot_reserved` is True, the caller already reserved the capacity
        under BAS and this method only performs the actual enqueue.
        """

        vertex = self.vertices.get(vertex_id)

        def _enqueue():
            if not slot_reserved:
                yield self.reserve_buffer_slot(vertex_id)

            yield vertex.buffer.put(tray)
            self._emit({
                'type': 'enqueued',
                't': self.env.now,
                'vertex_id': vertex_id,
                'tray_id': tray.id,
                'workpiece_id': tray.current_workpiece_id,
            })

        return self.env.process(_enqueue())

    def _emit(self, event: dict):
        normalized_event = dict(event)
        normalized_event.setdefault('t', self.env.now)

        if normalized_event.get('type') == 'tray_completed':
            tray_id = normalized_event.get('tray_id')
            if tray_id is not None and tray_id not in self._completed:
                self._completed.append(tray_id)

        for listener in self._listeners:
            try:
                listener(dict(normalized_event))
            except Exception as e:
                print(f"[emit] Event listener error ignored: {e}")

        try:
            print(normalized_event)
            file_exists = os.path.exists(self.out_log_csv_file)
            write_header = not file_exists or os.path.getsize(self.out_log_csv_file) == 0
            row = {
                'time': normalized_event.get('t', ""),
                'station_id': 'S' + str(normalized_event.get('vertex_id', "") if normalized_event.get('vertex_id', "") is not None else normalized_event.get('tail', "")),
                'tray_id': 'T' + str(normalized_event.get('tray_id', "")),
                'workpiece_id': 'P' + str(normalized_event.get('workpiece_id', "")),
                'activity': normalized_event.get('type', ""),
            }
            with open(self.out_log_csv_file, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except Exception as e:
            try:
                print(f"[emit] Logging error ignored: {e}")
            except Exception:
                pass

    def run(self, until: float | None):
        self.env.run(until=until)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run graph-based discrete-event simulation.')
    parser.add_argument('-g', '--graph-model-file', default=DEFAULT_GRAPH_MODEL_FILE,
                        help='Path to the system graph JSON file.')
    parser.add_argument('-o', '--out-log-csv-file', default=DEFAULT_OUT_LOG_CSV_FILE,
                        help='Path to the output CSV event log file.')
    parser.add_argument('-n', '--tray-number', type=int,
                        help='Number of trays to inject at start in non-serial mode.')
    parser.add_argument('-f', '--factor', type=float, default=0,
                        help='Simulation speed factor (sim seconds per real second), 0 for as fast as possible.')
    parser.add_argument('-s', '--serial', action='store_true',
                        help='Enable serial mode with continuous source injection.')
    parser.add_argument('--seed', type=int, default=None,
                        help='Random seed for the simulation (default: No seed fixed).')
    parser.add_argument('-t', '--end-time', type=float,
                        help='Simulation end time in simulated seconds.')
    parser.add_argument('-m', '--mes-control-mode', action=argparse.BooleanOptionalAction, default=False,
                        help='Enable/disable MES control mode (default: disabled).')
    parser.add_argument('--mes-host', default='localhost',
                        help='Hostname for station TCP clients (required when --mes-control-mode).')
    parser.add_argument('--mes-port', type=int, default=6789,
                        help='Port for station TCP clients (required when --mes-control-mode).')
    args = parser.parse_args()

    with open(args.graph_model_file, 'r') as f:
        graph_json = json.load(f)
    g = LabelledGraph('System Graph', graph_json)

    try:
        out_dir = os.path.dirname(args.out_log_csv_file) or "."
        os.makedirs(out_dir, exist_ok=True)
        if os.path.exists(args.out_log_csv_file):
            os.remove(args.out_log_csv_file)
    except Exception as e:
        print(f"[setup] Could not prepare log file '{args.out_log_csv_file}': {e}")

    if args.seed is not None:
        RandomFactory.set_seed(args.seed)

    if args.factor == 0:
        env = simpy.Environment()
    else:
        env = simpy.RealtimeEnvironment(factor=args.factor, strict=False)

    if args.mes_control_mode:
        if not args.mes_host or not args.mes_port:
            parser.error('--mes-host and --mes-port are required when --mes-control-mode is enabled')
    if not args.serial and args.tray_number is None:
        parser.error('--tray-number is required when --serial is not enabled')

    sim = GraphSimulation(
        graph=g,
        env=env,
        out_log_csv_file=args.out_log_csv_file,
        mes_control_mode=args.mes_control_mode,
        mes_host=args.mes_host,
        mes_port=args.mes_port,
    )

    if args.serial:
        sim.start_serial_injection()
    else:
        for source_vertex_id in sim.get_source_vertex_ids():
            for _ in range(args.tray_number):
                sim.inject_tray(spawn_vertex_id=source_vertex_id, at=0.0)

    seed = RandomFactory.get_seed()
    print(f"Simulation begins. Random seed used: {seed}")
    start_real = time.time()

    sim.run(until=args.end_time)

    end_real = time.time()
    elapsed_real = end_real - start_real
    sim_time = getattr(env, 'now', None)
    print(f"-------------------\n"
          f"Simulation finished. simulated-time={sim_time}, real-elapsed={elapsed_real:.3f}s, seed={seed}")
