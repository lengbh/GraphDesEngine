"""Travel SMP equivalence, shared degradation and physical-arrival checks."""

import contextlib
import io
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import simpy

from conveyor_smp import ConveyorRuntime
from labelled_graph import Arc, LabelledGraph
from random_util import RandomFactory
from sim_engine import GraphSimulation


def ideal_json(distribution):
    return {
        'initial_state': 'idle', 'states': ['idle', 'running'],
        'transition_probabilities': {'idle': {'running': 1.0}, 'running': {'idle': 1.0}},
        'holding_time_distributions': {'idle': {'running': None}, 'running': {'idle': distribution}},
    }


def condition_json(nominal=2, degraded=4, speed=0.5):
    return {
        'speed_factors': {'nominal': 1.0, 'degraded': speed},
        'smp': {
            'initial_state': 'nominal', 'states': ['nominal', 'degraded'],
            'transition_probabilities': {'nominal': {'degraded': 1.0}, 'degraded': {'nominal': 1.0}},
            'holding_time_distributions': {
                'nominal': {'degraded': {'type': 'constant', 'parameters': [nominal]}},
                'degraded': {'nominal': {'type': 'constant', 'parameters': [degraded]}},
            },
        },
    }


class ConveyorSMPTests(unittest.TestCase):
    def setUp(self):
        self.env = simpy.Environment()
        self.events = []
        with contextlib.redirect_stdout(io.StringIO()):
            RandomFactory.set_seed(20261117)

    def runtime(self, nominal_time=6, condition=None):
        model = {'tail': 1, 'head': 2,
                 'transfer_time_distribution': {'type': 'constant', 'parameters': [nominal_time]}}
        if condition is not None:
            model['conveyor_condition'] = condition
        self.arc = Arc(model)
        self.conveyor = ConveyorRuntime(self.env, self.arc, self.events.append)
        return self.conveyor

    def launch(self, tray_id, at=0):
        def trip():
            if at:
                yield self.env.timeout(at)
            nominal = self.conveyor.sample_transfer_time()
            transfer = self.conveyor.begin_transfer(tray_id, tray_id + 100, nominal)
            yield from self.conveyor.travel(transfer)
            self.conveyor.finish_admission(transfer)
        self.env.process(trip())

    def arrivals(self):
        return [(event['tray_id'], event['t']) for event in self.events
                if event['type'] == 'transfer_arrival']

    def step_until_arrival(self, maximum_steps=64):
        # A bounded event budget makes the old same-clock loop fail promptly.
        for _ in range(maximum_steps):
            if self.arrivals():
                return
            self.env.step()
        self.fail('Transfer did not arrive within a bounded number of events')

    def test_ideal_wrapper_preserves_samples_and_rng_position(self):
        distributions = [('normal', [15, 2]), ('uniform', [1, 50]), ('constant', [5]),
                         ('exponential', [8]), ('triangular', [1, 12, 4]), ('weibull', [2, 5])]
        for kind, parameters in distributions:
            with self.subTest(kind=kind), contextlib.redirect_stdout(io.StringIO()):
                RandomFactory.set_seed(82)
                reference = RandomFactory.get_random_generator(kind, parameters)
                samples = [reference() for _ in range(100)]
                next_random = RandomFactory._get_rng().random()
                RandomFactory.set_seed(82)
                arc = Arc({'tail': 1, 'head': 2,
                           'transfer_time_distribution': {'type': kind, 'parameters': parameters}})
                conveyor = ConveyorRuntime(self.env, arc, self.events.append)
                self.assertEqual(samples, [conveyor.sample_transfer_time() for _ in range(100)])
                self.assertEqual(next_random, RandomFactory._get_rng().random())

    def test_smp_only_and_legacy_arc_definitions_agree(self):
        distribution = {'type': 'constant', 'parameters': [7]}
        arc = Arc({'tail': 1, 'head': 2, 'smp': ideal_json(distribution)})
        self.assertEqual(arc.transfer(), 7)
        self.assertEqual(arc.smp.sample_transition('running').holding_time, 7)
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            Arc({'tail': 1, 'head': 2, 'smp': ideal_json(distribution),
                 'transfer_time_distribution': {'type': 'constant', 'parameters': [8]}})

    def test_ideal_transfers_overlap_without_serialising(self):
        self.runtime()
        self.launch(1)
        self.launch(2, at=1)
        self.env.run(until=8)
        self.assertEqual(self.arrivals(), [(1, 6), (2, 7)])
        transitions = [event for event in self.events if event['type'] == 'transfer_smp_transition']
        self.assertEqual([(e['tray_id'], e['state_from'], e['state_to'], e['holding_time'])
                          for e in transitions], [
            (1, 'idle', 'running', 0), (2, 'idle', 'running', 0),
            (1, 'running', 'idle', 6), (2, 'running', 'idle', 6),
        ])

    def test_shared_degradation_preserves_progress_without_resampling(self):
        self.runtime(condition=condition_json())
        with patch.object(self.arc.smp, 'sample_transition', wraps=self.arc.smp.sample_transition) as sample:
            self.launch(1)
            self.launch(2, at=1)
            self.env.run(until=11)
            # Each trip samples its external start and one nominal work duration.
            self.assertEqual(sample.call_count, 4)
        self.assertEqual(self.arrivals(), [(1, 8), (2, 10)])
        for tray_id in (1, 2):
            progress = [e['progress'] for e in self.events
                        if e['type'] == 'transfer_progress' and e['tray_id'] == tray_id]
            self.assertAlmostEqual(sum(progress), 6)
        changes = [e['t'] for e in self.events if e['type'] == 'conveyor_condition_transition']
        self.assertEqual(changes, [2, 6, 8])

    def test_condition_clock_continues_while_arc_has_no_trays(self):
        self.runtime(nominal_time=3, condition=condition_json())
        self.launch(1, at=5)
        self.env.run(until=10)
        self.assertEqual(self.arrivals(), [(1, 9)])
        self.assertEqual([e['t'] for e in self.events if e['type'] == 'conveyor_condition_transition'], [2, 6, 8])

    def test_zero_speed_pauses_and_resumes_remaining_work(self):
        self.runtime(nominal_time=3, condition=condition_json(speed=0))
        self.launch(1)
        self.env.run(until=8)
        self.assertEqual(self.arrivals(), [(1, 7)])

    def test_completion_at_condition_boundary_and_zero_work(self):
        self.runtime(nominal_time=2, condition=condition_json())
        self.launch(1)
        self.env.run(until=3)
        self.assertEqual(self.arrivals(), [(1, 2)])
        self.env = simpy.Environment()
        self.events = []
        self.runtime(nominal_time=0, condition=condition_json())
        self.launch(1)
        self.env.run(until=1)
        self.assertEqual(self.arrivals(), [(1, 0)])

    def test_processed_completion_consumes_the_observed_rounding_residual(self):
        start = 30743.004154520822
        work = 0.3105405431797041
        self.env = simpy.Environment(initial_time=start)
        self.runtime(nominal_time=work,
                     condition=condition_json(nominal=1000, degraded=1000, speed=.1))
        self.launch(1)
        self.step_until_arrival()
        self.assertEqual(self.arrivals(), [(1, start+work)])
        segments = [e for e in self.events if e['type'] == 'transfer_progress']
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]['remaining_work'], 0)
        self.assertEqual(segments[0]['progress'], work)

    def test_large_clock_interrupt_then_point_one_speed_completes(self):
        start = 1_000_000_000.125
        work = .600000003
        self.env = simpy.Environment(initial_time=start)
        self.runtime(nominal_time=work,
                     condition=condition_json(nominal=.5, degraded=1000, speed=.1))
        self.launch(1)
        self.env.run(until=start+.75)
        # The original completion was scheduled (and thus triggered), but did
        # not win the AnyOf when the condition changed after half a second.
        self.assertFalse(self.arrivals())
        active = next(iter(self.conveyor.active_transfers.values()))
        self.assertEqual(active.remaining_work, work-.5)
        self.step_until_arrival()
        expected = start+.5+(work-.5)/.1
        self.assertEqual(self.arrivals(), [(1, expected)])
        segments = [e for e in self.events if e['type'] == 'transfer_progress']
        self.assertEqual([e['speed_factor'] for e in segments], [1.0, .1])
        self.assertEqual(segments[-1]['remaining_work'], 0)
        self.assertEqual(math.fsum(e['progress'] for e in segments), work)

    def test_point_one_completion_tied_with_condition_change_at_large_clock(self):
        start = 1_000_000_000.125
        self.env = simpy.Environment(initial_time=start)
        condition = condition_json(nominal=1000, degraded=2, speed=.1)
        condition['smp']['initial_state'] = 'degraded'
        self.runtime(nominal_time=.2, condition=condition)
        self.launch(1)
        self.step_until_arrival()
        self.assertEqual(self.arrivals(), [(1, start+2)])
        segments = [e for e in self.events if e['type'] == 'transfer_progress']
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]['progress'], .2)
        self.assertEqual(segments[0]['remaining_work'], 0)
        self.assertEqual(segments[0]['condition_state'], 'degraded')

    def test_small_real_work_is_preserved_when_only_the_condition_changes(self):
        self.runtime(nominal_time=1,
                     condition=condition_json(nominal=1-5e-13, degraded=2, speed=0))
        self.launch(1)
        self.env.run(until=2)
        self.assertFalse(self.arrivals())
        active = next(iter(self.conveyor.active_transfers.values()))
        self.assertGreater(active.remaining_work, 0)
        self.step_until_arrival()
        self.assertEqual(self.arrivals(), [(1, 3)])
        segments = [e for e in self.events if e['type'] == 'transfer_progress']
        self.assertEqual(math.fsum(e['progress'] for e in segments), 1)

    def test_horizon_censoring_reports_unfinished_progress(self):
        self.runtime(condition=condition_json())
        self.launch(1)
        self.env.run(until=5)
        self.conveyor.observe('censored')
        censored = [e for e in self.events if e['type'] == 'transfer_smp_censored']
        self.assertEqual(len(censored), 1)
        self.assertEqual(censored[0]['remaining_work'], 2.5)
        self.assertEqual(censored[0]['holding_time'], 5)
        self.assertFalse(self.arrivals())

    def test_invalid_condition_definitions_fail(self):
        for speed in (-1, float('nan'), float('inf')):
            with self.subTest(speed=speed), self.assertRaisesRegex(ValueError, 'speed factors'):
                self.runtime(condition=condition_json(speed=speed))
        condition = condition_json()
        condition['speed_factors']['unknown'] = 1
        with self.assertRaisesRegex(ValueError, 'every condition state'):
            self.runtime(condition=condition)
        condition = condition_json()
        condition['smp']['holding_time_distributions']['nominal']['degraded'] = None
        with self.assertRaisesRegex(ValueError, 'autonomous timed'):
            self.runtime(condition=condition)

    def test_zero_condition_hold_fails_instead_of_spinning_at_one_time(self):
        self.runtime(condition=condition_json(nominal=0))
        with self.assertRaisesRegex(ValueError, 'strictly positive'):
            self.env.run(until=1)


class ConveyorIntegrationTests(unittest.TestCase):
    def test_unbounded_sweep_patch_still_applies_to_the_extended_runtime(self):
        import shutil
        import sys
        from experiments.run_smp_tray_sweep import capacity_smoke, patch_runtime
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as folder:
            runtime = Path(folder)
            for filename in ('sim_engine.py', 'labelled_graph.py', 'conveyor_smp.py',
                             'smp.py', 'random_util.py'):
                shutil.copy2(root / filename, runtime / filename)
            patch_runtime(runtime)
            result = capacity_smoke(runtime, sys.executable)
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['processing_capacity'], 1)
        self.assertGreaterEqual(result['maximum_observed_queue'], 9)

    def test_degraded_logs_feed_existing_station_verifier_and_window_parser(self):
        import sys
        import types
        from experiments.smp_challenge_logs import verify_run
        from experiments.smp_steady_state_metrics import extract_window_trace
        module = types.ModuleType('station_tcp_client')
        server_lines, tray_orders = [], {}
        next_order = 1
        class Client:
            def __init__(self, **kwargs):
                pass
            def request_action(self, station_id, tray_id):
                nonlocal next_order
                server_lines.append(f'Action query received: workstation_id: {station_id}, tray_id: {tray_id}')
                if station_id == 1 and tray_id not in tray_orders:
                    order = next_order
                    next_order += 1
                    tray_orders[tray_id] = order
                    server_lines.append(f'[ORDER] Order {order} of product type 1 assigned to tray {tray_id}')
                return tray_orders[tray_id], 1, 0
            def request_routing(self, station_id, tray_id):
                server_lines.append(f'Action done query received: workstation_id: {station_id}, tray_id: {tray_id}')
                if station_id == 2:
                    order = tray_orders.pop(tray_id)
                    server_lines.append(f'[ORDER] Order {order} marked as FINISHED')
                    return 0, 0, 1
                return tray_orders[tray_id], 0, 2
        module.StationTCPClient = Client
        graph_json = {
            'vertices': [
                {'id': station, 'name': str(station), 'buffer_capacity': 2,
                 'service_time_distribution': {'type': 'constant', 'parameters': [station]}}
                for station in (1, 2)
            ],
            'arcs': [
                {'tail': 1, 'head': 2, 'conveyor_condition': condition_json(),
                 'transfer_time_distribution': {'type': 'constant', 'parameters': [3]}},
                {'tail': 2, 'head': 1,
                 'transfer_time_distribution': {'type': 'constant', 'parameters': [1]}},
            ],
        }
        capabilities = {'stations': [{'id': station, 'process_capability': station} for station in (1, 2)]}
        products = {'products': [{'product_type': 1, 'processes': [{'process_id': 1}, {'process_id': 2}]}]}
        with tempfile.TemporaryDirectory() as folder, patch.dict(sys.modules, {'station_tcp_client': module}):
            output = Path(folder)
            env = simpy.Environment()
            events = []
            simulation = GraphSimulation(LabelledGraph('Verifier compatibility', graph_json), env,
                                         str(output / 'events.csv'), True, 'localhost', 6789,
                                         [events.append], str(output / 'events_detailed.csv'))
            for _ in range(2):
                simulation.inject_tray(1)
            with contextlib.redirect_stdout(io.StringIO()):
                simulation.run(until=30)
            (output / 'simulation_stdout.txt').write_text('Simulation finished.\n')
            (output / 'mes_stdout.txt').write_text('\n'.join(server_lines) + '\n')
            verification = verify_run(output, 30, graph_json, capabilities, products)
            trace = extract_window_trace(output, 30, graph_json, capabilities, products,
                                         warmup_seconds=5, verify=True)
        self.assertTrue(verification['verified'])
        self.assertGreater(verification['completed_orders'], 0)
        self.assertEqual(trace['duration_seconds'], 25)
        self.assertGreater(trace['orders_completed'], 0)
        self.assertTrue(any(e['type'] == 'conveyor_condition_transition' for e in events))
        for station in trace['stations'].values():
            self.assertAlmostEqual(sum(station['state_seconds'].values()), 25)
            self.assertEqual(station['state_seconds'].get('failed', 0), 0)

    def test_physical_arrival_is_separate_from_mes_downstream_admission(self):
        # Fake action/routing only: physical travel and admission use real engine paths.
        import sys
        import types
        module = types.ModuleType('station_tcp_client')
        class Client:
            def __init__(self, **kwargs):
                pass
            def request_action(self, station_id, tray_id):
                return 100 + tray_id, 1, 0
            def request_routing(self, station_id, tray_id):
                return 100 + tray_id, 0, 2
        module.StationTCPClient = Client
        graph_json = {
            'vertices': [
                {'id': 1, 'name': 'Source', 'vertex_role': 'source', 'buffer_capacity': 4,
                 'service_time_distribution': {'type': 'constant', 'parameters': [1]}},
                {'id': 2, 'name': 'Sink', 'vertex_role': 'sink', 'buffer_capacity': 1,
                 'service_time_distribution': {'type': 'constant', 'parameters': [100]}},
            ],
            'arcs': [{'tail': 1, 'head': 2,
                      'transfer_time_distribution': {'type': 'constant', 'parameters': [1]}}],
        }
        events = []
        with tempfile.TemporaryDirectory() as output, patch.dict(sys.modules, {'station_tcp_client': module}):
            env = simpy.Environment()
            simulation = GraphSimulation(LabelledGraph('Arrival versus admission', graph_json), env,
                                         str(Path(output) / 'events.csv'), True,
                                         'localhost', 6789, [events.append],
                                         str(Path(output) / 'details.csv'))
            for _ in range(4):
                simulation.inject_tray(1)
            with contextlib.redirect_stdout(io.StringIO()):
                simulation.run(until=105)
            arrivals = {e['tray_id']: e for e in events if e['type'] == 'transfer_arrival'}
            ends = {e['tray_id']: e for e in events if e['type'] == 'transfer_end'}
            self.assertEqual(arrivals[3]['t'], 4)
            self.assertEqual(ends[3]['t'], 102)
            self.assertEqual(ends[3]['admission_wait'], 98)
            self.assertEqual(ends[3]['travel_time'], 1)
            self.assertEqual(simulation.arcs[1, 2].active_transfers, {})
            import csv
            with (Path(output) / 'details.csv').open(newline='') as stream:
                rows = list(csv.DictReader(stream))
            row = next(row for row in rows if row['activity'] == 'transfer_end' and row['tray_id'] == 'T3')
            self.assertEqual(row['resource_type'], 'arc')
            self.assertEqual(row['transfer_id'], 'A1_2-3')
            self.assertEqual(row['admission_wait'], '98.0')


if __name__ == '__main__':
    unittest.main()
