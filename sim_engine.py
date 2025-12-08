import simpy
from typing import Callable
from dataclasses import dataclass
import json
import csv
import os
import time
import argparse
from station_tcp_client import StationTCPClient
from simpy.resources.store import StorePut
from labelled_graph import Vertex, Arc, LabelledGraph
from random_util import RandomFactory

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
    def __init__(self,
                 env: simpy.Environment,
                 vertex: Vertex,
                 get_arc_to_next_vertex: Callable[[int, int], Arc],
                 transfer_to_next_vertex: Callable[[int, Tray], StorePut],
                 emit_event: Callable[[dict], None],
                 client: StationTCPClient = None):
        self.env = env
        self.vertex = vertex
        if client is None:
            # TODO replace with routing probabilities
            self.request_action = lambda _, __: (0, 1, 0)
            self.request_next_vertex = lambda current_vertex, __: (0, 0, (2 if current_vertex == 1 else 1))
        else:
            self.request_action = client.request_action
            self.request_next_vertex = client.request_routing
        self.get_arc_to_next_vertex = get_arc_to_next_vertex
        self.emit_event = emit_event
        self.buffer = simpy.Store(env, capacity=vertex.buffer_capacity)
        self.server = simpy.Resource(env, capacity=2)
        self.transfer_to_next_vertex = transfer_to_next_vertex

        self.action = env.process(self._worker())

    def _worker(self):
        while True:
            tray = yield self.buffer.get()  # TODO fetch time
            self.emit_event({'type': 'dequeued', 't': self.env.now,
                             'vertex_id': self.vertex.id, 'tray_id': tray.id, 'workpiece_id': tray.current_workpiece_id})

            order_id, action, next_v = self.request_action(self.vertex.id, tray.id)
            tray.current_workpiece_id = order_id

            ## TODO no error and exception handling for now

            if action == 0:
                arc_to_next_vertex = self.get_arc_to_next_vertex(self.vertex.id, next_v)
                transfer_time = arc_to_next_vertex.transfer()
                self.env.process(self._transfer_process(next_v, tray, transfer_time))

            else:
                with self.server.request() as req:
                    yield req
                    service_time = self.vertex.service()
                    self.emit_event({'type': 'service_start', 't': self.env.now, 'vertex_id': self.vertex.id,
                                     'tray_id': tray.id, 'workpiece_id': tray.current_workpiece_id, 'service_time': service_time})
                    yield self.env.timeout(service_time)
                    self.emit_event({'type': 'service_end', 't': self.env.now,
                                     'vertex_id': self.vertex.id, 'tray_id': tray.id, 'workpiece_id': tray.current_workpiece_id})

                    order_id, action, next_v = self.request_next_vertex(self.vertex.id, tray.id)
                    tray.current_workpiece_id = order_id
                    assert action == 0
                    arc_to_next_vertex = self.get_arc_to_next_vertex(self.vertex.id, next_v)
                    transfer_time = arc_to_next_vertex.transfer()
                    self.env.process(self._transfer_process(next_v, tray, transfer_time))

    def _transfer_process(self, next_vertex: int, tray: Tray, transfer_time: float):
        # Emit start immediately in this process, then wait transfer time and enqueue
        self.emit_event({'type': 'transfer_start', 't': self.env.now, 'tray_id': tray.id, 'workpiece_id': tray.current_workpiece_id,
                         'tail': self.vertex.id, 'head': next_vertex, 'transfer_time': transfer_time})
        yield self.env.timeout(transfer_time)
        self.emit_event({'type': 'transfer_end', 't': self.env.now, 'tray_id': tray.id, 'workpiece_id': tray.current_workpiece_id,
                         'tail': self.vertex.id, 'head': next_vertex})
        # Enqueue to next vertex; don't block on the put event here
        self.transfer_to_next_vertex(next_vertex, tray)


class GraphSimulation:
    def __init__(self, graph: LabelledGraph, env: simpy.Environment, out_log_csv_file: str,
                 mes_control_mode: bool, mes_host: str = None, mes_port: int = None):
        self.graph = graph
        self.env = env
        self.out_log_csv_file = out_log_csv_file
        self.vertices: dict[int, VertexRuntime] = {}
        # TODO add listener functions for ending conditions (associated to arg batch number)
        # self._listeners: list[Callable[[dict], None]] = []
        self._next_tray_id = 1
        self._trays: dict[int, Tray] = {}
        self._completed: list[int] = []

        # Instantiate vertices for simulation
        if mes_control_mode:
            self._clients: dict[int, StationTCPClient] = {}
            for vertex in self.graph.vertices.values():
                # TODO configure server host
                client = StationTCPClient(env=self.env, host=mes_host, port=mes_port, timeout=60)
                self._clients[vertex.id] = client
                self.vertices[vertex.id] = VertexRuntime(self.env, vertex,
                                                         get_arc_to_next_vertex=self.graph.get_arc,
                                                         transfer_to_next_vertex=self.transfer_to_next_vertex,
                                                         emit_event=self._emit,
                                                         client=client)
        else:
            for vertex in self.graph.vertices.values():
                self.vertices[vertex.id] = VertexRuntime(self.env, vertex,
                                                         get_arc_to_next_vertex=self.graph.get_arc,
                                                         transfer_to_next_vertex=self.transfer_to_next_vertex,
                                                         emit_event=self._emit,
                                                         client=None)

        self.env.process(self._completion_monitor())

    def _completion_monitor(self):
        while True:
            yield self.env.timeout(1.0)
            # TODO check ending conditions using listener, could be assigned by an msg by mes
            pass

    def inject_tray(self, spawn_vertex_id: int, at: float = 0) -> int:
        tray_id = self._next_tray_id
        self._next_tray_id += 1
        tray = Tray(id=tray_id, current_workpiece_id=0)
        self._trays[tray_id] = tray

        def _spawn():
            if at > 0:
                yield self.env.timeout(at)
            vertex = self.vertices.get(spawn_vertex_id)
            if vertex is None:
                tray.completed_at = self.env.now
                self._completed.append(tray_id)
                self._emit({'type': 'tray_completed', 't': self.env.now, 'vertex_id': spawn_vertex_id,
                            'tray_id': tray_id, 'workpiece_id': tray.current_workpiece_id})
                return

            self._emit({'type': 'injected', 'vertex_id': spawn_vertex_id, 'tray_id': tray_id, 'workpiece_id': tray.current_workpiece_id})
            # Add to buffer
            yield self.transfer_to_next_vertex(spawn_vertex_id, tray)

        self.env.process(_spawn())
        return tray_id

    def transfer_to_next_vertex(self, vertex_id: int, tray: Tray) -> StorePut:
        vertex = self.vertices.get(vertex_id)
        ev = vertex.buffer.put(tray)

        def _after_put():
            yield ev
            self._emit({'type': 'enqueued', 't': self.env.now, 'vertex_id': vertex_id, 'tray_id': tray.id, 'workpiece_id': tray.current_workpiece_id})

        self.env.process(_after_put())
        return ev

    def _emit(self, event: dict):
        try:
            print(event)
            file_exists = os.path.exists(self.out_log_csv_file)
            write_header = not file_exists or os.path.getsize(self.out_log_csv_file) == 0
            # row = {k: event.get(k, "") for k in CSV_FIELDS}
            row = {
                'time': event.get('t', ""),
                'station_id': 'S' + str(event.get('vertex_id', "") if event.get('vertex_id', "") is not None else event.get('tail', "")),
                'tray_id': 'T' + str(event.get('tray_id', "")),
                'workpiece_id': 'P' + str(event.get('workpiece_id', "")),
                'activity': event.get('type', ""),
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
                        help='Number of trays to inject at start.')
    parser.add_argument('-f', '--factor', type=float, default=0,
                        help='Simulation speed factor (sim seconds per real second), 0 for as fast as possible.')
    parser.add_argument('-s', '--seed', type=int, default=None,
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

    # Load graph model
    with open(args.graph_model_file, 'r') as f:
        graph_json = json.load(f)
    g = LabelledGraph('System Graph', graph_json)

    # Prepare log file
    try:
        out_dir = os.path.dirname(args.out_log_csv_file) or "."
        os.makedirs(out_dir, exist_ok=True)
        if os.path.exists(args.out_log_csv_file):
            os.remove(args.out_log_csv_file)
    except Exception as e:
        print(f"[setup] Could not prepare log file '{args.out_log_csv_file}': {e}")

    # Set up simulation environment
    if args.seed is not None:
        RandomFactory.set_seed(args.seed)

    if args.factor == 0:
        env = simpy.Environment()
    else:
        env = simpy.RealtimeEnvironment(factor=args.factor, strict=False)

    if args.mes_control_mode:
        if not args.mes_host or not args.mes_port:
            parser.error('--mes-host and --mes-port are required when --mes-control-mode is enabled')
    sim = GraphSimulation(graph=g, env=env, out_log_csv_file=args.out_log_csv_file,
                          mes_control_mode=args.mes_control_mode, mes_host=args.mes_host, mes_port=args.mes_port)

    # Inject trays at time 0
    for _ in range(args.tray_number):
        sim.inject_tray(spawn_vertex_id=1, at=0.0)

    # Print seed and measure real (wall-clock) time taken by the simulation run
    seed = RandomFactory.get_seed()
    print(f"Simulation begins. Random seed used: {seed}")
    start_real = time.time()
    # Run the simulation (simulated time). When this returns, simulation is complete.
    # TODO: for MES, send order batch and receive batch completion message
    sim.run(until=args.end_time)
    end_real = time.time()

    elapsed_real = end_real - start_real
    # Also show the simulation's final clock value
    sim_time = getattr(env, 'now', None)
    print(f"-------------------\n"
          f"Simulation finished. simulated-time={sim_time}, real-elapsed={elapsed_real:.3f}s, seed={seed}")
