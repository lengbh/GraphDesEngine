"""SMP configuration loading and compatibility with existing service timing."""

import contextlib
import copy
import io
from pathlib import Path
import tempfile
import unittest
from typing import Any

import simpy

from labelled_graph import LabelledGraph, Vertex
from random_util import RandomFactory
from sim_engine import GraphSimulation
from smp import SMP


def ideal_model(distribution: dict[str, Any]) -> dict[str, Any]:
    return {
        "initial_state": "idle",
        "states": ["idle", "running"],
        "transition_probabilities": {"idle": {"running": 1.0}, "running": {"idle": 1.0}},
        "holding_time_distributions": {"idle": {"running": None}, "running": {"idle": distribution}},
    }


def station() -> dict[str, Any]:
    return {"id": 1, "name": "Station", "vertex_role": "sink", "buffer_capacity": 2}


def seed():
    with contextlib.redirect_stdout(io.StringIO()):
        RandomFactory.set_seed(2027)


class SMPConfigTests(unittest.TestCase):
    def setUp(self):
        seed()

    def test_smp_only_station_uses_configured_running_time(self):
        definition = station()
        definition["smp"] = ideal_model({"type": "constant", "parameters": [7]})
        graph = LabelledGraph("Configured SMP", {"vertices": [definition], "arcs": []})
        events = []
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            sim = GraphSimulation(graph, simpy.Environment(), str(Path(directory) / "events.csv"),
                                  mes_control_mode=False, event_listeners=[events.append])
            self.assertIs(sim.vertices[1].smp, graph.vertices[1].smp)
            sim.inject_tray(1, at=3)
            sim.run(until=11)
        self.assertEqual([(event["type"], event["t"]) for event in events
                          if event["type"] in ("service_start", "service_end")],
                         [("service_start", 3), ("service_end", 10)])
        self.assertEqual(sim.vertices[1].smp_state, "idle")

    def test_legacy_and_configured_smp_preserve_all_distribution_samples(self):
        distributions = [
            ("constant", [3]), ("normal", [5, 0.5]), ("uniform", [2, 5]),
            ("exponential", [3]), ("triangular", [2, 6, 4]), ("weibull", [2, 3]),
        ]
        for kind, parameters in distributions:
            with self.subTest(distribution=kind):
                distribution = {"type": kind, "parameters": parameters}
                seed()
                legacy = Vertex(dict(station(), service_time_distribution=distribution))
                expected = [legacy.service() for _ in range(20)]
                next_draw = RandomFactory._get_rng().random()
                seed()
                configured = Vertex(dict(station(), smp=ideal_model(distribution)))
                actual = []
                for _ in range(20):
                    self.assertIsNone(configured.smp.sample_transition("idle").holding_time)
                    actual.append(configured.smp.sample_transition("running").holding_time)
                self.assertEqual(actual, expected)
                self.assertEqual(RandomFactory._get_rng().random(), next_draw)
                self.assertEqual(configured.service_distribution_type, kind)
                self.assertEqual(configured.service_distribution_params, parameters)

    def test_station_rejects_unsupported_execution_semantics(self):
        valid = ideal_model({"type": "constant", "parameters": [2]})
        initial_running = copy.deepcopy(valid)
        initial_running["initial_state"] = "running"
        timed_idle = copy.deepcopy(valid)
        timed_idle["holding_time_distributions"]["idle"]["running"] = {"type": "constant", "parameters": [1]}
        controlled_running = copy.deepcopy(valid)
        controlled_running["holding_time_distributions"]["running"]["idle"] = None
        self_loop = copy.deepcopy(valid)
        self_loop["transition_probabilities"]["running"] = {"running": 1.0}
        self_loop["holding_time_distributions"]["running"] = {"running": {"type": "constant", "parameters": [1]}}
        for model in (initial_running, timed_idle, controlled_running, self_loop):
            with self.subTest(model=model), self.assertRaises(ValueError):
                Vertex(dict(station(), smp=model))

    def test_rejects_conflicting_legacy_and_smp_distributions(self):
        with self.assertRaisesRegex(ValueError, "conflicting"):
            Vertex(dict(station(), smp=ideal_model({"type": "constant", "parameters": [7]}),
                        service_time_distribution={"type": "constant", "parameters": [2]}))

    def test_matching_legacy_and_smp_distributions_are_accepted(self):
        distribution = {"type": "constant", "parameters": [7]}
        vertex = Vertex(dict(station(), smp=ideal_model(distribution), service_time_distribution=distribution))
        self.assertEqual(vertex.service(), 7)

    def test_invalid_json_model_is_rejected(self):
        invalid_models: list[Any] = [{}, None, {"states": []}]
        for model in invalid_models:
            with self.subTest(model=model), self.assertRaises(ValueError):
                SMP.from_json(model)
        missing_distribution = ideal_model({"type": "constant", "parameters": [2]})
        del missing_distribution["holding_time_distributions"]["running"]["idle"]
        with self.assertRaisesRegex(ValueError, "Missing holding-time sampler"):
            SMP.from_json(missing_distribution)

    def test_repairable_model_samples_conditional_running_and_repair_times(self):
        model = ideal_model({'type': 'constant', 'parameters': [10]})
        model['states'].append('failed')
        model['transition_probabilities']['running'] = {'idle': 0.97, 'failed': 0.03}
        model['transition_probabilities']['failed'] = {'running': 1.0}
        model['holding_time_distributions']['running']['failed'] = {'type': 'constant', 'parameters': [4]}
        model['holding_time_distributions']['failed'] = {'running': {'type': 'uniform', 'parameters': [6, 8]}}
        vertex = Vertex(dict(station(), smp=model))
        sampled = [vertex.smp.sample_transition('running') for _ in range(1000)]
        failures = [transition for transition in sampled if transition.next_state == 'failed']
        self.assertTrue(10 < len(failures) < 60)
        self.assertTrue(all(t.holding_time == (4 if t.next_state == 'failed' else 10) for t in sampled))
        repairs = [vertex.smp.sample_transition('failed') for _ in range(20)]
        self.assertTrue(all(t.next_state == 'running' and t.holding_time is not None
                            and 6 <= t.holding_time <= 8 for t in repairs))
        broken = copy.deepcopy(model)
        broken['holding_time_distributions']['failed']['running'] = None
        with self.assertRaisesRegex(ValueError, 'repair and restart'):
            Vertex(dict(station(), smp=broken))


if __name__ == "__main__":
    unittest.main()
