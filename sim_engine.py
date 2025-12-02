import simpy
from typing import Callable
from dataclasses import dataclass
import json
import csv
import os
from station_tcp_client import StationTCPClient
from simpy.resources.store import StorePut
from labelled_graph import Vertex, Arc, LabelledGraph
from random_util import RandomFactory

GRAPH_MODEL_FILE = 'SystemGraphs/two_station_circular_system_graph.json'
OUT_LOG_CSV_FILE = 'EventLogs/event_logs.csv'
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
            self.request_action = lambda _, __: (1, 0)
            self.request_next_vertex = lambda current_vertex, __: (0, (2 if current_vertex == 1 else 1))
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
    def __init__(self, graph: LabelledGraph, env: simpy.Environment, mes_control_mode: bool):
        self.graph = graph
        self.env = env
        self.vertices: dict[int, VertexRuntime] = {}
        # TODO add listener functions
        # self._listeners: list[Callable[[dict], None]] = []
        self._next_tray_id = 1
        self._trays: dict[int, Tray] = {}
        self._completed: list[int] = []

        # Instantiate vertices for simulation
        if mes_control_mode:
            self._clients: dict[int, StationTCPClient] = {}
            for vertex in self.graph.vertices.values():
                # TODO configure server host
                client = StationTCPClient(env=self.env, host='localhost', port=6789, timeout=60)
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

    @staticmethod
    def _emit(event: dict):
        try:
            print(event)
            file_exists = os.path.exists(OUT_LOG_CSV_FILE)
            write_header = not file_exists or os.path.getsize(OUT_LOG_CSV_FILE) == 0
            # row = {k: event.get(k, "") for k in CSV_FIELDS}
            row = {
                'time': event.get('t', ""),
                'station_id': 'S' + str(event.get('vertex_id', "") if event.get('vertex_id', "") is not None else event.get('tail', "")),
                'tray_id': 'T' + str(event.get('tray_id', "")),
                'workpiece_id': 'P' + str(event.get('workpiece_id', "")),
                'activity': event.get('type', ""),
            }
            with open(OUT_LOG_CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
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
    RandomFactory.set_seed(1762904783269162000)

    with open(GRAPH_MODEL_FILE, 'r') as f:
        graph_json = json.load(f)
    g = LabelledGraph('System Graph', graph_json)

    if os.path.exists(OUT_LOG_CSV_FILE):
        os.remove(OUT_LOG_CSV_FILE)

    # env = simpy.RealtimeEnvironment(factor=0.1, strict=False)
    env = simpy.Environment()
    sim = GraphSimulation(g, env=env, mes_control_mode=False)

    sim.inject_tray(spawn_vertex_id=1, at=0.0)
    sim.inject_tray(spawn_vertex_id=1, at=0.0)
    sim.inject_tray(spawn_vertex_id=1, at=0.0)

    sim.run(until=1000)
