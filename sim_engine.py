import numpy as np
import simpy
from typing import Callable
from dataclasses import dataclass

from simpy.resources.store import StorePut
from labelled_graph import Vertex, Arc, LabelledGraph


@dataclass
class Workpiece:
    id: int
    created_at: float
    completed_at: float
    path: list[int]


class NodeRuntime:
    def __init__(self,
                 env: simpy.Environment,
                 vertex: Vertex,
                 get_next_vertex: Callable[[int], int],
                 get_arc_to_next_vertex: Callable[[int, int], Arc],
                 transfer_to_next_vertex: Callable[[int, Workpiece], StorePut],
                 emit_event: Callable[[dict], None]):
        self.env = env
        self.vertex = vertex
        self.get_next_vertex = get_next_vertex
        self.get_arc_to_next_vertex = get_arc_to_next_vertex
        self.buffer = simpy.Store(env, capacity=vertex.buffer_capacity)
        self.server = simpy.Resource(env, capacity=1)
        self.action = env.process(self._worker())
        self.emit_event = emit_event
        self.transfer_to_next_vertex = transfer_to_next_vertex

    @staticmethod
    def safe_time(t: float) -> float:
        if not np.isfinite(t):
            return 0.0
        return max(0.0, float(t))

    def _worker(self):
        while True:
            workpiece = yield self.buffer.get()     # TODO physical moving time from buffer to workstation
            workpiece.path.append(self.vertex.id)
            self.emit_event({'type': 'dequeued', 't': self.env.now, 'vertex_id': self.vertex.id})

            with self.server.request() as req:
                yield req
                service_time = self.safe_time(self.vertex.service())
                self.emit_event({'type': 'service_start', 't': self.env.now,
                                 'vertex_id': self.vertex.id, 'service_time': service_time})
                yield self.env.timeout(service_time)
                self.emit_event({'type': 'service_end', 't': self.env.now, 'vertex_id': self.vertex.id})

            # TODO to be replaced by MES control
            next_vertex = self.get_next_vertex(self.vertex.id)
            arc_to_next_vertex = self.get_arc_to_next_vertex(self.vertex.id, next_vertex)
            transfer_time = self.safe_time(arc_to_next_vertex.transfer())

            self.emit_event({'type': 'transfer_start', 't': self.env.now, 'tail': self.vertex.id, 'head': next_vertex, 'transfer_time': transfer_time})
            yield self.env.timeout(transfer_time)
            self.emit_event({'type': 'transfer_end', 't': self.env.now, 'tail': self.vertex.id, 'head': next_vertex})

            yield self.transfer_to_next_vertex(next_vertex, workpiece)


class GraphSimulation:
    def __init__(self, graph: LabelledGraph, env: simpy.Environment = None):
        self.graph = graph
        self.env = env if env is not None else simpy.Environment()

        self.nodes: dict[int, NodeRuntime] = {}

        # TODO add listener functions
        # self._listeners: list[Callable[[dict], None]] = []

        self._next_workpiece_id = 1
        self._workpieces: dict[int, Workpiece] = {}
        self._completed: list[int] = []

        for vertex in self.graph.vertices.values():
            self.nodes[vertex.id] = NodeRuntime(self.env, vertex,
                    get_next_vertex= lambda current_vertex: 2 if current_vertex == 1 else 1,
                    get_arc_to_next_vertex=self.graph.get_arc,
                    transfer_to_next_vertex=self.transfer_to_next_vertex,
                    emit_event=self._emit)

        self.env.process(self._completion_monitor())

    def _completion_monitor(self):
        while True:
            yield self.env.timeout(1.0)
            # TODO check ending conditions
            pass

    def inject_workpiece(self, start_vertex_id: int, at: float = 0) -> int:
        workpiece_id = self._next_workpiece_id
        self._next_workpiece_id += 1
        workpiece = Workpiece(id=workpiece_id, created_at=self.env.now + max(0.0, at), completed_at=0.0, path=[])
        self._workpieces[workpiece_id] = workpiece  # TODO remove finished workpieces

        def _spawn():
            if at > 0:
                yield self.env.timeout(at)
            node = self.nodes.get(start_vertex_id)
            if node is None:
                workpiece.completed_at = self.env.now
                self._completed.append(workpiece_id)
                self._emit({'type': 'workpiece_completed', 't': self.env.now, 'vertex_id': start_vertex_id, 'workpiece_id': workpiece_id})
                return

            self._emit({'type': 'injected', 'vertex_id': start_vertex_id, 'workpiece_id': workpiece_id})
            # Add to buffer
            yield self.transfer_to_next_vertex(start_vertex_id, workpiece)

        self.env.process(_spawn())
        return workpiece_id

    def transfer_to_next_vertex(self, vertex_id: int, workpiece: Workpiece) -> StorePut:
        node = self.nodes.get(vertex_id)
        ev = node.buffer.put(workpiece)
        def _after_put():
            yield ev
            self._emit({'type': 'enqueued', 't': self.env.now, 'vertex_id': vertex_id, 'workpiece_id': workpiece.id})
        self.env.process(_after_put())
        return ev

    @staticmethod
    def _emit(event: dict):
        print(event)    # TODO

    def run(self, until: float):
        self.env.run(until=until)


def build_environment(graph: LabelledGraph) -> GraphSimulation:
    return GraphSimulation(graph)

def build_realtime_environment(graph: LabelledGraph, factor: float = 1.0, strict: bool = False) -> GraphSimulation:
    """
    Build a GraphSimulation that runs in realtime with a slowdown/speedup factor.
    (<1 to speed up; >1 to slow down)
    """
    from simpy.rt import RealtimeEnvironment
    env = RealtimeEnvironment(factor=factor, strict=strict)
    return GraphSimulation(graph, env=env)


if __name__ == '__main__':
    import json
    from labelled_graph import GRAPH_MODEL_EXAMPLE_FILE

    with open(GRAPH_MODEL_EXAMPLE_FILE, 'r') as f:
        graph_json = json.load(f)
    g = LabelledGraph('Example Graph', graph_json)
    sim = build_realtime_environment(g, 0.1)

    # Inject 1 workpiece for now
    sim.inject_workpiece(start_vertex_id=1, at=0.0)

    sim.run(until=100)
