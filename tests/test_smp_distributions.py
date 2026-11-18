"""Distribution checks for arbitrary local timing and aggregate graph controls."""

import contextlib
import io
import math
import unittest

import numpy as np

from random_util import RandomFactory
from smp import SMP


class FlexibleHoldingTimeTests(unittest.TestCase):
    def setUp(self):
        with contextlib.redirect_stdout(io.StringIO()):
            RandomFactory.set_seed(20261106)

    def test_empirical_preserves_observed_masses_including_repeated_values(self):
        sampler = RandomFactory.get_random_generator('empirical', [1, 1, 1, 10])
        samples = [sampler() for _ in range(20000)]
        self.assertEqual(set(samples), {1.0, 10.0})
        self.assertAlmostEqual(samples.count(10) / len(samples), .25, delta=.012)

    def test_mixture_retains_the_gap_and_component_probability(self):
        sampler = RandomFactory.get_random_generator('uniform_mixture', [.8, 10, 20, 100, 120])
        samples = np.asarray([sampler() for _ in range(20000)])
        self.assertTrue(np.all(((10 <= samples) & (samples <= 20)) | ((100 <= samples) & (samples <= 120))))
        self.assertAlmostEqual(float(np.mean(samples < 50)), .8, delta=.012)
        self.assertAlmostEqual(float(np.mean(samples)), .8 * 15 + .2 * 110, delta=1)

    def test_invalid_observations_and_mixture_parameters_are_rejected(self):
        invalid: list[tuple[str, list[float]]] = [
            ('empirical', []), ('empirical', [-1]), ('empirical', [math.inf]),
            ('uniform_mixture', [2, 1, 2, 3, 4]), ('uniform_mixture', [.5, 2, 1, 3, 4]),
            ('uniform_mixture', [.5, 1, 2, -1, 4]), ('uniform_mixture', [.5, 1, 2, 3]),
        ]
        for kind, parameters in invalid:
            with self.subTest(kind=kind, parameters=parameters), self.assertRaises(ValueError):
                RandomFactory.get_random_generator(kind, parameters)

    def test_empirical_smp_remains_a_timed_service_with_external_idle(self):
        model = SMP.from_json({
            'initial_state': 'idle', 'states': ['idle', 'running'],
            'transition_probabilities': {'idle': {'running': 1}, 'running': {'idle': 1}},
            'holding_time_distributions': {'idle': {'running': None}, 'running': {
                'idle': {'type': 'empirical', 'parameters': [10, 100]}}},
        })
        model.require_station_model()
        self.assertIsNone(model.sample_transition('idle').holding_time)
        self.assertEqual({model.sample_transition('running').holding_time for _ in range(50)}, {10, 100})

    def test_lognormal_parameters_describe_the_underlying_normal(self):
        expected = np.random.default_rng(20261106).lognormal(2.0, .5, 100)
        sampler = RandomFactory.get_random_generator('lognormal', [2.0, .5])
        np.testing.assert_array_equal([sampler() for _ in range(100)], expected)
        deterministic = RandomFactory.get_random_generator('lognormal', [2.0, 0.0])
        self.assertEqual(deterministic(), math.exp(2.0))

    def test_lognormal_rejects_invalid_parameters(self):
        for parameters in ([], [1], [1, 2, 3], [1, -.1], [math.inf, 1],
                           [1, math.nan], [True, 1], ['1', 1]):
            with self.subTest(parameters=parameters), self.assertRaises(ValueError):
                RandomFactory.get_random_generator('lognormal', parameters)


if __name__ == '__main__':
    unittest.main()
