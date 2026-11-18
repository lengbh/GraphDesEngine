"""Integration checks for ideal SMP service timing and existing control flow."""

import contextlib
import csv
import io
from pathlib import Path
import sys
import tempfile
import types
from typing import Any
import unittest
from unittest.mock import patch

import simpy

from labelled_graph import LabelledGraph
from random_util import RandomFactory
from sim_engine import GraphSimulation
from smp import SMPTransition


def two_station_graph(source_time=2, sink_time=3, source_capacity=2, transfer_time=1):
    return {
        "vertices": [
            {
                "id": 1,
                "name": "Source",
                "vertex_role": "source",
                "buffer_capacity": source_capacity,
                "service_time_distribution": {"type": "constant", "parameters": [source_time]},
            },
            {
                "id": 2,
                "name": "Sink",
                "vertex_role": "sink",
                "buffer_capacity": 1,
                "service_time_distribution": {"type": "constant", "parameters": [sink_time]},
            },
        ],
        "arcs": [
            {
                "tail": 1,
                "head": 2,
                "transfer_time_distribution": {"type": "constant", "parameters": [transfer_time]},
            },
        ],
    }


class SMPServiceTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self._output_dir: tempfile.TemporaryDirectory[str] | None = None
        self.events: list[dict[str, Any]] = []
        self.states_at_events: list[dict[int, str]] = []
        self.mes_requests: list[tuple[str, float, int, int, str]] = []
        self.env = simpy.Environment()
        self._sim: GraphSimulation | None = None

    @property
    def output_dir(self) -> tempfile.TemporaryDirectory[str]:
        if self._output_dir is None:
            raise RuntimeError("Test fixtures have not been set up")
        return self._output_dir

    @property
    def sim(self) -> GraphSimulation:
        if self._sim is None:
            raise RuntimeError("Call make_simulation before accessing the simulation")
        return self._sim

    def setUp(self):
        self._output_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.output_dir.cleanup)
        self.events = []
        self.states_at_events = []
        self.mes_requests = []
        with contextlib.redirect_stdout(io.StringIO()):
            RandomFactory.set_seed(2027)

    def make_simulation(self, graph_json=None, mes_mode=False, bypass_source=False, detailed=False):
        self.env = simpy.Environment()
        graph = LabelledGraph("SMP service test", graph_json or two_station_graph())

        def record_event(event):
            self.events.append(event)
            self.states_at_events.append({
                vertex_id: runtime.smp_state for vertex_id, runtime in self.sim.vertices.items()
            })

        def record_request(kind, station_id, tray_id):
            self.mes_requests.append((
                kind, self.env.now, station_id, tray_id, self.sim.vertices[station_id].smp_state,
            ))

        class FakeMESClient:
            def __init__(self, **kwargs):
                pass

            def request_action(self, station_id, tray_id):
                record_request("action", station_id, tray_id)
                if bypass_source and station_id == 1:
                    return (100 + tray_id, 0, 2)
                return (100 + tray_id, 1, 0)

            def request_routing(self, station_id, tray_id):
                record_request("routing", station_id, tray_id)
                return (100 + tray_id, 0, 2)

        # Exercise the MES client construction path without opening sockets.
        client_module = types.ModuleType("station_tcp_client")
        client_module.__dict__["StationTCPClient"] = FakeMESClient
        with patch.dict(sys.modules, {"station_tcp_client": client_module}):
            self._sim = GraphSimulation(
                graph, self.env, str(Path(self.output_dir.name) / "events.csv"),
                mes_control_mode=mes_mode, mes_host="localhost", mes_port=6789,
                event_listeners=[record_event],
                out_detail_csv_file=str(Path(self.output_dir.name) / 'details.csv') if detailed else None,
            )
        return self.sim

    def run_until(self, time):
        with contextlib.redirect_stdout(io.StringIO()):
            self.sim.run(until=time)

    def test_station_waits_idle_then_runs_for_existing_service_time(self):
        self.make_simulation()
        self.sim.inject_tray(1, at=3)
        self.assertEqual(self.sim.vertices[1].smp_state, "idle")
        self.run_until(2)
        self.assertEqual(self.sim.vertices[1].smp_state, "idle")
        self.assertFalse(any(event["type"] == "service_start" for event in self.events))
        self.run_until(4)
        self.assertEqual(self.sim.vertices[1].smp_state, "running")
        self.run_until(10)

        service_events = [
            (event["vertex_id"], event["type"], event["t"])
            for event in self.events if event["type"] in ("service_start", "service_end")
        ]
        self.assertEqual(service_events, [
            (1, "service_start", 3), (1, "service_end", 5),
            (2, "service_start", 6), (2, "service_end", 9),
        ])
        for event, states in zip(self.events, self.states_at_events):
            if event["type"] == "service_start":
                self.assertEqual(states[event["vertex_id"]], "running")
            elif event["type"] == "service_end":
                self.assertEqual(states[event["vertex_id"]], "idle")
        self.assertTrue(all(runtime.smp_state == "idle" for runtime in self.sim.vertices.values()))
        self.assertEqual(self.sim._completed, [1])

    def test_idle_after_processing_does_not_bypass_existing_blocking(self):
        self.make_simulation(two_station_graph(
            source_time=1, sink_time=100, source_capacity=8, transfer_time=2,
        ))
        for _ in range(8):
            self.sim.inject_tray(1)
        self.run_until(20)

        source_completions = [
            event for event in self.events
            if event["type"] == "service_end" and event["vertex_id"] == 1
        ]
        transfers = [event for event in self.events if event["type"] == "transfer_start"]
        self.assertEqual(len(source_completions), 3)
        self.assertEqual(len(transfers), 2)
        self.assertEqual(self.sim.vertices[1].smp_state, "idle")
        self.assertEqual(self.sim.vertices[2].smp_state, "running")
        self.assertEqual(len(self.sim.vertices[2].buffer.items), 1)

    def test_mes_execute_and_routing_queries_keep_their_existing_order(self):
        self.make_simulation(mes_mode=True)
        self.sim.inject_tray(1)
        self.run_until(10)

        self.assertEqual(self.mes_requests, [
            ("action", 0, 1, 1, "idle"),
            ("routing", 2, 1, 1, "idle"),
            ("action", 3, 2, 1, "idle"),
        ])
        completion = [event for event in self.events if event["type"] == "tray_completed"]
        self.assertEqual(len(completion), 1)
        self.assertEqual(completion[0]["t"], 6)

    def test_mes_mode_requires_host_and_port(self):
        graph = LabelledGraph("MES connection validation", two_station_graph())
        for host, port in ((None, None), (None, 6789), ("localhost", None), ("", 6789)):
            with self.subTest(host=host, port=port):
                with self.assertRaisesRegex(ValueError, "MES host and port"):
                    GraphSimulation(
                        graph, simpy.Environment(), str(Path(self.output_dir.name) / "events.csv"),
                        mes_control_mode=True, mes_host=host, mes_port=port,
                    )

    def test_missing_mes_action_response_stops_before_service(self):
        self.make_simulation(mes_mode=True)
        with patch.object(self.sim.vertices[1], "request_action", return_value=(None, None, None)):
            self.sim.inject_tray(1)
            with self.assertRaisesRegex(RuntimeError, "No valid action response for station 1, tray 1"):
                self.run_until(10)
        self.assertEqual(self.sim.vertices[1].smp_state, "idle")
        self.assertFalse(any(event["type"] == "service_start" for event in self.events))
        self.assertFalse(any(event["type"] == "transfer_start" for event in self.events))

    def test_missing_mes_routing_response_stops_before_transfer(self):
        self.make_simulation(mes_mode=True)
        with patch.object(self.sim.vertices[1], "request_next_vertex", return_value=(None, None, None)):
            self.sim.inject_tray(1)
            with self.assertRaisesRegex(RuntimeError, "No valid routing response for station 1, tray 1"):
                self.run_until(10)
        self.assertEqual(self.sim.vertices[1].smp_state, "idle")
        self.assertTrue(any(event["type"] == "service_end" for event in self.events))
        self.assertFalse(any(event["type"] == "transfer_start" for event in self.events))

    def test_mes_release_bypasses_service_without_sampling_smp(self):
        self.make_simulation(mes_mode=True, bypass_source=True)
        source_smp = self.sim.vertices[1].smp
        with patch.object(source_smp, "sample_transition", wraps=source_smp.sample_transition) as sample:
            self.sim.inject_tray(1)
            self.run_until(10)
            sample.assert_not_called()

        self.assertTrue(all(states[1] == "idle" for states in self.states_at_events))
        self.assertEqual(self.mes_requests, [
            ("action", 0, 1, 1, "idle"), ("action", 1, 2, 1, "idle"),
        ])
        completion = [event for event in self.events if event["type"] == "tray_completed"]
        self.assertEqual(len(completion), 1)
        self.assertEqual(completion[0]["t"], 4)

    def test_failure_holds_tray_and_restarts_full_service_before_reporting_to_mes(self):
        self.make_simulation(two_station_graph(source_time=10), mes_mode=True, detailed=True)
        source = self.sim.vertices[1]
        transitions = [
            SMPTransition('idle', 'running', None),
            SMPTransition('running', 'failed', 4),
            SMPTransition('failed', 'running', 6),
            SMPTransition('running', 'failed', 4),
            SMPTransition('failed', 'running', 6),
            SMPTransition('running', 'idle', 10),
        ]
        with patch.object(source.smp, 'sample_transition', side_effect=transitions):
            self.sim.inject_tray(1)
            # Another tray cannot start while the first tray is being repaired.
            self.sim.inject_tray(1, at=1)
            self.run_until(5)
            self.assertEqual(source.smp_state, 'failed')
            self.assertEqual(source.server.count, 1)
            self.assertEqual(len(source.buffer.items), 1)
            self.assertEqual(self.mes_requests, [('action', 0, 1, 1, 'idle')])
            self.assertFalse(any(event['type'] in ('service_end', 'transfer_start') for event in self.events))
            self.run_until(29)
            self.assertEqual(source.smp_state, 'running')
            self.assertEqual(self.mes_requests, [('action', 0, 1, 1, 'idle')])
            # Stop at successful completion before the next tray samples service.
            self.run_until(30)
        self.run_until(35)
        first_operation = [event for event in self.events if event.get('service_id') == 'S1-1']
        self.assertEqual([(e['type'], e['t']) for e in first_operation
                          if e['type'] in ('service_start', 'service_restart', 'service_end')], [
            ('service_start', 0), ('service_restart', 10), ('service_restart', 20), ('service_end', 30),
        ])
        self.assertEqual([(e['state_from'], e['state_to'], e['holding_time']) for e in first_operation
                          if e['type'] == 'smp_transition'], [
            ('idle', 'running', 0), ('running', 'failed', 4), ('failed', 'running', 6),
            ('running', 'failed', 4), ('failed', 'running', 6), ('running', 'idle', 10),
        ])
        self.assertEqual(self.mes_requests[:4], [
            ('action', 0, 1, 1, 'idle'), ('routing', 30, 1, 1, 'idle'),
            ('action', 30, 1, 2, 'idle'), ('action', 31, 2, 1, 'idle'),
        ])
        with (Path(self.output_dir.name) / 'details.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        failures = [row for row in rows if row['activity'] == 'smp_transition' and row['state_to'] == 'failed']
        self.assertEqual(len(failures), 2)
        self.assertTrue(all(row['tray_id'] == 'T1' and row['workpiece_id'] == 'P101'
                            and row['service_id'] == 'S1-1' and row['holding_time'] == '4.0'
                            for row in failures))

    def test_horizon_during_repair_logs_censored_state_without_completion(self):
        self.make_simulation(mes_mode=True, detailed=True)
        source = self.sim.vertices[1]
        with patch.object(source.smp, 'sample_transition', side_effect=[
            SMPTransition('idle', 'running', None),
            SMPTransition('running', 'failed', 2),
            SMPTransition('failed', 'running', 20),
        ]):
            self.sim.inject_tray(1)
            self.run_until(10)
        self.assertEqual(source.smp_state, 'failed')
        self.assertEqual(self.mes_requests, [('action', 0, 1, 1, 'idle')])
        self.assertFalse(any(e['type'] in ('failure_end', 'service_end', 'transfer_start') for e in self.events))
        censored = [e for e in self.events if e['type'] == 'smp_censored' and e['vertex_id'] == 1]
        self.assertEqual([(e['state_to'], e['holding_time']) for e in censored], [('failed', 8)])

    def test_detailed_logging_failure_stops_instead_of_losing_state_data(self):
        self.make_simulation()
        self.sim.out_detail_csv_file = str(Path(self.output_dir.name) / 'missing' / 'details.csv')
        with self.assertRaises(FileNotFoundError):
            self.run_until(1)


if __name__ == "__main__":
    unittest.main()
