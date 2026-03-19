import argparse
import json
import math
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import simpy

from labelled_graph import LabelledGraph
from random_util import RandomFactory
from sim_engine import GraphSimulation


DEFAULT_GRAPH_MODEL_FILE = "SystemGraphs/two_station_circular_system_graph.json"
DEFAULT_OUT_LOG_CSV_FILE = "EventLogs/live_visualizer_event_logs.csv"


def compute_layout(graph: LabelledGraph) -> tuple[dict[int, dict[str, float]], list[dict[str, int]]]:
    vertex_ids = sorted(graph.vertices.keys())
    count = max(1, len(vertex_ids))
    radius = 220
    center_x = 340
    center_y = 260
    positions: dict[int, dict[str, float]] = {}

    for index, vertex_id in enumerate(vertex_ids):
        angle = (2 * math.pi * index / count) - (math.pi / 2)
        positions[vertex_id] = {
            "x": center_x + radius * math.cos(angle),
            "y": center_y + radius * math.sin(angle),
        }

    edges = []
    for arc in graph.arcs.values():
        edges.append({"tail": arc.tail, "head": arc.head})

    return positions, edges


class LiveSystemState:
    def __init__(self, graph: LabelledGraph):
        positions, edges = compute_layout(graph)
        self._lock = threading.Lock()
        self._snapshot = {
            "simulation_running": True,
            "simulation_time": 0.0,
            "event_count": 0,
            "vertices": {
                str(vertex.id): {
                    "id": vertex.id,
                    "name": vertex.name,
                    "buffer_capacity": vertex.buffer_capacity,
                    "buffer": [],
                    "in_service": [],
                    "pending_release": [],
                    "x": positions[vertex.id]["x"],
                    "y": positions[vertex.id]["y"],
                }
                for vertex in graph.vertices.values()
            },
            "edges": [
                {
                    "tail": edge["tail"],
                    "head": edge["head"],
                    "transferring": [],
                }
                for edge in edges
            ],
            "completed": [],
            "last_event": None,
        }
        self._edge_index = {
            (edge["tail"], edge["head"]): edge
            for edge in self._snapshot["edges"]
        }

    def apply_event(self, event: dict):
        with self._lock:
            snapshot = self._snapshot
            snapshot["simulation_time"] = max(snapshot["simulation_time"], float(event.get("t", 0.0)))
            snapshot["event_count"] += 1
            snapshot["last_event"] = {
                "type": event.get("type", ""),
                "time": float(event.get("t", 0.0)),
                "tray_id": event.get("tray_id"),
                "vertex_id": event.get("vertex_id"),
                "tail": event.get("tail"),
                "head": event.get("head"),
            }

            event_type = event.get("type")
            tray_id = self._format_tray_id(event.get("tray_id"))
            vertex_id = event.get("vertex_id")
            vertex = snapshot["vertices"].get(str(vertex_id)) if vertex_id is not None else None

            if event_type == "enqueued" and vertex is not None:
                self._append_unique(vertex["buffer"], tray_id)
            elif event_type == "dequeued" and vertex is not None:
                self._remove_if_present(vertex["buffer"], tray_id)
            elif event_type == "service_start" and vertex is not None:
                self._remove_if_present(vertex["pending_release"], tray_id)
                self._append_unique(vertex["in_service"], tray_id)
            elif event_type == "service_end" and vertex is not None:
                self._remove_if_present(vertex["in_service"], tray_id)
                self._append_unique(vertex["pending_release"], tray_id)
            elif event_type == "transfer_start":
                tail = event.get("tail")
                head = event.get("head")
                edge = self._edge_index.get((tail, head))
                tail_vertex = snapshot["vertices"].get(str(tail))
                if tail_vertex is not None:
                    self._remove_if_present(tail_vertex["in_service"], tray_id)
                    self._remove_if_present(tail_vertex["pending_release"], tray_id)
                    self._remove_if_present(tail_vertex["buffer"], tray_id)
                if edge is not None:
                    self._append_unique(edge["transferring"], tray_id)
            elif event_type == "transfer_end":
                tail = event.get("tail")
                head = event.get("head")
                edge = self._edge_index.get((tail, head))
                if edge is not None:
                    self._remove_if_present(edge["transferring"], tray_id)
            elif event_type == "tray_completed":
                self._append_unique(snapshot["completed"], tray_id)

    def mark_complete(self):
        with self._lock:
            self._snapshot["simulation_running"] = False

    def snapshot(self) -> dict:
        with self._lock:
            return json.loads(json.dumps(self._snapshot))

    @staticmethod
    def _format_tray_id(tray_id) -> str:
        return f"T{tray_id}" if tray_id not in (None, "") else ""

    @staticmethod
    def _append_unique(values: list[str], value: str):
        if value and value not in values:
            values.append(value)

    @staticmethod
    def _remove_if_present(values: list[str], value: str):
        if value in values:
            values.remove(value)


def build_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>GraphDesEngine Live Visualizer</title>
  <style>
    :root {
      --bg: #f5efe5;
      --panel: rgba(255, 250, 242, 0.9);
      --ink: #23313a;
      --muted: #667784;
      --frame: #cdbda7;
      --buffer: #8fb8de;
      --service: #5d8a66;
      --pending: #b86c4e;
      --transfer: #d99152;
      --done: #9a8b78;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, #efdfc8 0, transparent 30%),
        linear-gradient(180deg, #faf4eb 0%, var(--bg) 100%);
    }
    main {
      max-width: 1280px;
      margin: 0 auto;
      padding: 24px;
    }
    h1 {
      margin: 0 0 8px;
      font-size: 2rem;
    }
    p {
      margin: 0 0 16px;
      color: var(--muted);
    }
    .stats {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 18px;
    }
    .card {
      min-width: 170px;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--frame);
      background: var(--panel);
      box-shadow: 0 14px 34px rgba(75, 60, 32, 0.08);
    }
    .card .label {
      display: block;
      color: var(--muted);
      font-size: 0.85rem;
      margin-bottom: 4px;
    }
    .card .value {
      font-size: 1.25rem;
      font-weight: 700;
    }
    .layout {
      display: grid;
      grid-template-columns: 1.4fr 0.9fr;
      gap: 18px;
    }
    .panel {
      border: 1px solid var(--frame);
      background: var(--panel);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 14px 34px rgba(75, 60, 32, 0.08);
    }
    .legend {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 10px;
      color: var(--muted);
      font-size: 0.92rem;
    }
    .legend-item {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .swatch {
      width: 14px;
      height: 14px;
      border-radius: 4px;
      display: inline-block;
    }
    svg {
      width: 100%;
      height: 560px;
      display: block;
    }
    .station-card {
      border: 1px solid #ddd0bb;
      border-radius: 14px;
      padding: 12px;
      background: rgba(255, 255, 255, 0.52);
    }
    .station-card + .station-card {
      margin-top: 10px;
    }
    .station-title {
      font-weight: 700;
      margin-bottom: 8px;
    }
    .chips {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      margin-top: 6px;
    }
    .chip {
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 0.8rem;
      color: white;
      background: var(--muted);
    }
    .chip.buffer { background: var(--buffer); color: #17324b; }
    .chip.service { background: var(--service); }
    .chip.pending { background: var(--pending); }
    .chip.transfer { background: var(--transfer); }
    .chip.done { background: var(--done); }
    .empty {
      color: var(--muted);
      font-size: 0.9rem;
    }
    @media (max-width: 960px) {
      .layout { grid-template-columns: 1fr; }
      svg { height: 460px; }
    }
  </style>
</head>
<body>
  <main>
    <h1>GraphDesEngine Live Visualizer</h1>
    <p>Buffer trays, active service, and transfers are refreshed live from the running simulation.</p>
    <section class="stats">
      <div class="card"><span class="label">Simulation Time</span><span class="value" id="sim-time">0.00</span></div>
      <div class="card"><span class="label">Events Processed</span><span class="value" id="event-count">0</span></div>
      <div class="card"><span class="label">Status</span><span class="value" id="sim-status">Starting</span></div>
      <div class="card"><span class="label">Last Event</span><span class="value" id="last-event">n/a</span></div>
    </section>
    <section class="layout">
      <div class="panel">
        <div class="legend">
          <span class="legend-item"><span class="swatch" style="background:var(--buffer)"></span>Buffer</span>
          <span class="legend-item"><span class="swatch" style="background:var(--service)"></span>Service</span>
          <span class="legend-item"><span class="swatch" style="background:var(--pending)"></span>Pending Release</span>
          <span class="legend-item"><span class="swatch" style="background:var(--transfer)"></span>Transfer</span>
          <span class="legend-item"><span class="swatch" style="background:var(--done)"></span>Completed</span>
        </div>
        <svg id="graph" viewBox="0 0 680 520" role="img" aria-label="Live system graph"></svg>
      </div>
      <div class="panel">
        <div id="station-list"></div>
        <div class="station-card" style="margin-top:10px;">
          <div class="station-title">Completed Trays</div>
          <div class="chips" id="completed-list"></div>
        </div>
      </div>
    </section>
  </main>
  <script>
    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function renderChips(items, kind) {
      if (!items || items.length === 0) {
        return '<div class="empty">None</div>';
      }
      return `<div class="chips">${items.map(item => `<span class="chip ${kind}">${escapeHtml(item)}</span>`).join("")}</div>`;
    }

    function renderGraph(state) {
      const svg = document.getElementById("graph");
      const vertices = Object.values(state.vertices);
      const edgeLines = state.edges.map(edge => {
        const tail = state.vertices[String(edge.tail)];
        const head = state.vertices[String(edge.head)];
        const midX = (tail.x + head.x) / 2;
        const midY = (tail.y + head.y) / 2;
        const transferText = edge.transferring.length ? edge.transferring.join(", ") : "";
        return `
          <line x1="${tail.x}" y1="${tail.y}" x2="${head.x}" y2="${head.y}" stroke="#b9aa96" stroke-width="4" />
          <text x="${midX}" y="${midY - 12}" font-size="12" text-anchor="middle" fill="#7b6f62">${escapeHtml(`S${edge.tail} -> S${edge.head}`)}</text>
          <rect x="${midX - 52}" y="${midY - 2}" width="104" height="24" rx="12" fill="rgba(217,145,82,0.18)" stroke="rgba(217,145,82,0.35)"></rect>
          <text x="${midX}" y="${midY + 14}" font-size="12" text-anchor="middle" fill="#9b5a20">${escapeHtml(transferText || "idle")}</text>
        `;
      }).join("");

      const vertexNodes = vertices.map(vertex => {
        const bufferSummary = vertex.buffer.length ? vertex.buffer.join(", ") : "empty";
        const serviceSummary = vertex.in_service.length ? vertex.in_service.join(", ") : "idle";
        const pendingSummary = vertex.pending_release.length ? vertex.pending_release.join(", ") : "none";
        return `
          <g>
            <rect x="${vertex.x - 82}" y="${vertex.y - 66}" width="164" height="132" rx="20" fill="rgba(255,250,242,0.35)" stroke="#bfae97" stroke-width="2"></rect>
            <text x="${vertex.x}" y="${vertex.y - 34}" font-size="16" font-weight="700" text-anchor="middle" fill="#23313a">${escapeHtml(`S${vertex.id}`)}</text>
            <text x="${vertex.x}" y="${vertex.y - 14}" font-size="12" text-anchor="middle" fill="#667784">${escapeHtml(vertex.name)}</text>
            <text x="${vertex.x}" y="${vertex.y + 10}" font-size="12" text-anchor="middle" fill="#32506c">${escapeHtml(`Buffer ${vertex.buffer.length}/${vertex.buffer_capacity}: ${bufferSummary}`)}</text>
            <text x="${vertex.x}" y="${vertex.y + 30}" font-size="12" text-anchor="middle" fill="#3f6447">${escapeHtml(`Service: ${serviceSummary}`)}</text>
            <text x="${vertex.x}" y="${vertex.y + 50}" font-size="12" text-anchor="middle" fill="#8b4c32">${escapeHtml(`Pending release: ${pendingSummary}`)}</text>
          </g>
        `;
      }).join("");

      svg.innerHTML = `${edgeLines}${vertexNodes}`;
    }

    function renderStationList(state) {
      const host = document.getElementById("station-list");
      const vertices = Object.values(state.vertices).sort((a, b) => a.id - b.id);
      host.innerHTML = vertices.map(vertex => `
        <div class="station-card">
          <div class="station-title">${escapeHtml(`S${vertex.id} ${vertex.name}`)}</div>
          <div>Buffer (${vertex.buffer.length}/${vertex.buffer_capacity})</div>
          ${renderChips(vertex.buffer, "buffer")}
          <div style="margin-top:8px;">In Service (${vertex.in_service.length})</div>
          ${renderChips(vertex.in_service, "service")}
          <div style="margin-top:8px;">Pending Release (${vertex.pending_release.length})</div>
          ${renderChips(vertex.pending_release, "pending")}
        </div>
      `).join("");

      document.getElementById("completed-list").innerHTML = state.completed.length
        ? state.completed.map(item => `<span class="chip done">${escapeHtml(item)}</span>`).join("")
        : '<div class="empty">None</div>';
    }

    async function refresh() {
      const response = await fetch("/state", { cache: "no-store" });
      const state = await response.json();
      document.getElementById("sim-time").textContent = state.simulation_time.toFixed(2);
      document.getElementById("event-count").textContent = String(state.event_count);
      document.getElementById("sim-status").textContent = state.simulation_running ? "Running" : "Completed";
      document.getElementById("last-event").textContent = state.last_event
        ? `${state.last_event.type} @ ${state.last_event.time.toFixed(2)}`
        : "n/a";
      renderGraph(state);
      renderStationList(state);
    }

    refresh();
    setInterval(() => {
      refresh().catch(error => console.error(error));
    }, 250);
  </script>
</body>
</html>
"""


class VisualizerServer(ThreadingHTTPServer):
    def __init__(self, server_address, request_handler_class, live_state: LiveSystemState):
        super().__init__(server_address, request_handler_class)
        self.live_state = live_state
        self.index_html = build_html().encode("utf-8")


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(self.server.index_html)))
            self.end_headers()
            self.wfile.write(self.server.index_html)
            return

        if parsed.path == "/state":
            payload = json.dumps(self.server.live_state.snapshot()).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, format, *args):
        return


def run_simulation(args, live_state: LiveSystemState):
    with open(args.graph_model_file, "r", encoding="utf-8") as handle:
        graph_json = json.load(handle)
    graph = LabelledGraph("System Graph", graph_json)

    out_dir = os.path.dirname(args.out_log_csv_file) or "."
    os.makedirs(out_dir, exist_ok=True)
    if os.path.exists(args.out_log_csv_file):
        os.remove(args.out_log_csv_file)

    if args.seed is not None:
        RandomFactory.set_seed(args.seed)

    env = simpy.RealtimeEnvironment(factor=args.factor, strict=False)
    sim = GraphSimulation(
        graph=graph,
        env=env,
        out_log_csv_file=args.out_log_csv_file,
        mes_control_mode=args.mes_control_mode,
        mes_host=args.mes_host,
        mes_port=args.mes_port,
        event_listeners=[live_state.apply_event],
    )

    for _ in range(args.tray_number):
        sim.inject_tray(spawn_vertex_id=1, at=0.0)

    try:
        sim.run(until=args.end_time)
    finally:
        live_state.mark_complete()


def parse_args():
    parser = argparse.ArgumentParser(description="Run a live browser-based visualizer for GraphDesEngine.")
    parser.add_argument("-g", "--graph-model-file", default=DEFAULT_GRAPH_MODEL_FILE,
                        help="Path to the system graph JSON file.")
    parser.add_argument("-o", "--out-log-csv-file", default=DEFAULT_OUT_LOG_CSV_FILE,
                        help="Path to the output CSV event log file.")
    parser.add_argument("-n", "--tray-number", type=int,
                        help="Number of trays to inject at start.")
    parser.add_argument("-f", "--factor", type=float, default=0,
                        help="Simulation speed factor (sim seconds per real second), 0 for as fast as possible.")
    parser.add_argument("-s", "--seed", type=int, default=None,
                        help="Random seed for the simulation (default: No seed fixed).")
    parser.add_argument("-t", "--end-time", type=float,
                        help="Simulation end time in simulated seconds.")
    parser.add_argument("-m", "--mes-control-mode", action=argparse.BooleanOptionalAction, default=False,
                        help="Enable/disable MES control mode (default: disabled).")
    parser.add_argument("--mes-host", default="localhost",
                        help="Hostname for station TCP clients (required when --mes-control-mode).")
    parser.add_argument("--mes-port", type=int, default=6789,
                        help="Port for station TCP clients (required when --mes-control-mode).")
    parser.add_argument("--host", default="127.0.0.1",
                        help="HTTP host for the live visualizer.")
    parser.add_argument("--port", type=int, default=8765,
                        help="HTTP port for the live visualizer.")
    args = parser.parse_args()

    if args.factor == 0:
        parser.error("--factor must be > 0 for live visualization")
    if args.mes_control_mode and (not args.mes_host or not args.mes_port):
        parser.error("--mes-host and --mes-port are required when --mes-control-mode is enabled")

    return args


def main():
    args = parse_args()

    with open(args.graph_model_file, "r", encoding="utf-8") as handle:
        graph_json = json.load(handle)
    graph = LabelledGraph("System Graph", graph_json)
    live_state = LiveSystemState(graph)

    server = VisualizerServer((args.host, args.port), RequestHandler, live_state)
    simulation_thread = threading.Thread(target=run_simulation, args=(args, live_state), daemon=True)
    simulation_thread.start()

    print(f"Live visualizer available at http://{args.host}:{args.port}")
    print("Open that URL in a browser while the simulation is running.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
