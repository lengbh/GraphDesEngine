import numpy as np
from typing import Callable
import time


class RandomFactory:
    @staticmethod
    def get_random_generator_from_json(distribution_json: dict, seed: int = None) -> Callable[[], float]:
        seed_used = seed if seed is not None else time.time_ns()
        rng = np.random.default_rng(seed_used)

        if distribution_json["type"] == "uniform":
            if len(distribution_json["parameters"]) != 2:
                raise ValueError("Uniform distribution requires exactly 2 parameters: low and high")
            low = distribution_json["parameters"][0]
            high = distribution_json["parameters"][1]
            return lambda: rng.uniform(low, high)

        if distribution_json["type"] == "normal":
            if len(distribution_json["parameters"]) != 2:
                raise ValueError("Normal distribution requires exactly 2 parameters: mean and stddev")
            mean = distribution_json["parameters"][0]
            stddev = distribution_json["parameters"][1]
            return lambda: rng.normal(mean, stddev)

        if distribution_json["type"] == "constant":
            if len(distribution_json["parameters"]) != 1:
                raise ValueError("Constant distribution requires exactly 1 parameter: value")
            value = distribution_json["parameters"][0]
            return lambda: value

        if distribution_json["type"] == "exponential":
            if len(distribution_json["parameters"]) != 1:
                raise ValueError("Exponential distribution requires exactly 1 parameter: mean")
            mean = distribution_json["parameters"][0]
            return lambda: rng.exponential(mean)

        if distribution_json["type"] == "triangular":
            if len(distribution_json["parameters"]) != 3:
                raise ValueError("Triangular distribution requires exactly 3 parameters: left, right, mode")
            left = distribution_json["parameters"][0]
            right = distribution_json["parameters"][1]
            mode = distribution_json["parameters"][2]
            return lambda: rng.triangular(left, mode, right)

        if distribution_json["type"] == "weibull":
            if len(distribution_json["parameters"]) != 2:
                raise ValueError("Weibull distribution requires exactly 2 parameter: shape, scale")
            shape = distribution_json["parameters"][0]
            scale = distribution_json["parameters"][1]
            return lambda: rng.weibull(shape) * scale

        raise ValueError("Unknown/unsupported distribution type")
