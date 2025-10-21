import json
from typing import Callable
from random_util import RandomFactory

GRAPH_MODEL_EXAMPLE_FILE = "graph_model_example.json"

class Vertex:
    def __init__(self, vertex_json: dict):
        self.id: int = vertex_json["id"]
        self.name: str = vertex_json["name"]
        self.buffer_capacity: int = vertex_json["buffer_capacity"]
        self.service_distribution_type: str = vertex_json["service_time_distribution"]["type"]
        self.service_distribution_params: list[float] = vertex_json["service_time_distribution"]["parameters"]
        self.service: Callable[[], float] \
            = RandomFactory.get_random_generator_from_json(vertex_json["service_time_distribution"])

    def __str__(self):
        return ((f"Vertex {self.id}: name={self.name}, buffer_capacity={self.buffer_capacity}, \n"
                f"service_time_distribution={self.service_distribution_type}{self.service_distribution_params}, \n"
                f"sample_service_time={self.service()} \n"))

class Arc:
    def __init__(self, arc_json: dict):
        self.tail: int = arc_json["tail"]
        self.head: int = arc_json["head"]
        self.transfer_time_distribution_type: str = arc_json["transfer_time_distribution"]["type"]
        self.transfer_time_distribution_params: list[float] = arc_json["transfer_time_distribution"]["parameters"]
        self.transfer: Callable[[], float] \
            = RandomFactory.get_random_generator_from_json(arc_json["transfer_time_distribution"])

    def __str__(self):
        return ((f"Arc from {self.tail} to {self.head}, \n"
                f"transfer_time_distribution={self.transfer_time_distribution_type}"
                f"{self.transfer_time_distribution_params}, \n"
                f"sample_transfer_time={self.transfer()} \n"))



class LabelledGraph:
    def __init__(self, name: str, graph_json: dict):
        self.name: str = name
        self.vertices: dict[int, Vertex] = {}
        self.arcs: dict[tuple[int, int], Arc] = {}
        for vertex_json in graph_json["vertices"]:
            vertex = Vertex(vertex_json)
            self.vertices[vertex.id] = vertex
        for arc_json in graph_json["arcs"]:
            arc = Arc(arc_json)
            self.arcs[arc.tail, arc.head] = arc

    def get_vertex(self, vertex_id: int) -> Vertex:
        return self.vertices[vertex_id]

    def get_arc(self, tail: int, head: int) -> Arc:
        return self.arcs[tail, head]

    def __str__(self):
        vertices_str = "\n".join(str(v) for v in self.vertices.values())
        arcs_str = "\n".join(str(a) for a in self.arcs)
        return (f"{'=' * 50}\n"
                f"Labelled graph \"{self.name}\" has {len(self.vertices)} vertices and {len(self.arcs)} arcs.\n\n"
                f"{vertices_str}\n{arcs_str}\n"
                f"{'=' * 50}")


if __name__ == "__main__":
    with open(GRAPH_MODEL_EXAMPLE_FILE, "r") as f:
        graph_json = json.load(f)

    graph = LabelledGraph("Example Graph", graph_json)
    print(graph)


