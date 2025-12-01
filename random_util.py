import numpy as np
from typing import Callable
import time


def safe_time(t: float) -> float:
    if not np.isfinite(t):
        return 0.0
    return max(0.0, float(t))


class RandomFactory:
    _seed: int | None = None
    _rng: np.random.Generator | None = None

    @classmethod
    def set_seed(cls, seed: int) -> None:
        cls._seed = int(seed)
        cls._rng = np.random.default_rng(cls._seed)
        try:
            print(f"Random seed used in this simulation run: {cls._seed}")
        except Exception:
            pass

    @classmethod
    def get_seed(cls) -> int:
        if cls._seed is None:
            cls._seed = time.time_ns()
            cls._rng = np.random.default_rng(cls._seed)
            try:
                print(f"Loaded random seed for this simulation run: {cls._seed}")
            except Exception:
                pass
        return cls._seed

    @classmethod
    def _get_rng(cls, seed: int | None = None) -> np.random.Generator:
        if cls._rng is None:
            if seed is not None:
                cls.set_seed(seed)
            else:
                cls.get_seed()
        return cls._rng  # type: ignore[return-value]

    @classmethod
    def get_random_generator(cls, time_distribution_type: str, time_distribution_params: list[float], seed: int | None = None) -> Callable[[], float]:
        rng = cls._get_rng(seed)
        dtype = str(time_distribution_type).lower()
        params = list(time_distribution_params)

        if dtype == "uniform":
            if len(params) != 2:
                raise ValueError("Uniform distribution requires exactly 2 parameters: low and high")
            low, high = params
            return lambda: safe_time(rng.uniform(low, high))

        if dtype == "normal":
            if len(params) != 2:
                raise ValueError("Normal distribution requires exactly 2 parameters: mean and stddev")
            mean, stddev = params
            return lambda: safe_time(rng.normal(mean, stddev))

        if dtype == "constant":
            if len(params) != 1:
                raise ValueError("Constant distribution requires exactly 1 parameter: value")
            (value,) = params
            return lambda: safe_time(float(value))

        if dtype == "exponential":
            if len(params) != 1:
                raise ValueError("Exponential distribution requires exactly 1 parameter: mean")
            (mean,) = params
            return lambda: safe_time(rng.exponential(mean))

        if dtype == "triangular":
            if len(params) != 3:
                raise ValueError("Triangular distribution requires exactly 3 parameters: left, right, mode")
            left, right, mode = params[0], params[1], params[2]
            return lambda: safe_time(rng.triangular(left, mode, right))

        if dtype == "weibull":
            if len(params) != 2:
                raise ValueError("Weibull distribution requires exactly 2 parameter: shape, scale")
            shape, scale = params
            return lambda: safe_time(rng.weibull(shape) * scale)

        raise ValueError(f"Unknown/unsupported distribution type: {time_distribution_type}")
