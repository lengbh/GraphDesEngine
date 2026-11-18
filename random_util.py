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
        rng = cls._rng
        assert rng is not None
        return rng

    @classmethod
    def get_random_generator(cls, time_distribution_type: str, time_distribution_params: list[float], seed: int | None = None) -> Callable[[], float]:
        rng = cls._get_rng(seed)
        dtype = str(time_distribution_type).lower()
        params = list(time_distribution_params)

        if dtype == "empirical":
            # Equal-mass observations; repeated values retain their frequency.
            values = np.asarray(params, dtype=float)
            if values.ndim != 1 or not len(values) or not np.all(np.isfinite(values)) or np.any(values < 0):
                raise ValueError("Empirical distribution requires finite nonnegative observations")
            return lambda: float(rng.choice(values))

        if dtype == "uniform_mixture":
            # A two-component continuous mixture, with P(component 1) = weight.
            if len(params) != 5 or not all(np.isfinite(value) for value in params):
                raise ValueError("Uniform mixture requires weight, low1, high1, low2, high2")
            weight, low1, high1, low2, high2 = params
            if not (0 <= weight <= 1 and 0 <= low1 <= high1 and 0 <= low2 <= high2):
                raise ValueError("Invalid uniform mixture weight or interval bounds")

            def sample_mixture() -> float:
                low, high = (low1, high1) if rng.random() < weight else (low2, high2)
                return float(rng.uniform(low, high))

            return sample_mixture

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

        if dtype == "lognormal":
            if len(params) != 2:
                raise ValueError("Lognormal distribution requires exactly 2 parameters: mu and sigma of the underlying normal")
            mu, sigma = params
            if not (isinstance(mu, (int, float)) and not isinstance(mu, bool)
                    and isinstance(sigma, (int, float)) and not isinstance(sigma, bool)
                    and np.isfinite(mu) and np.isfinite(sigma) and sigma >= 0):
                raise ValueError("Lognormal requires finite mu and finite nonnegative sigma")
            return lambda: safe_time(rng.lognormal(mu, sigma))

        if dtype == "weibull":
            if len(params) != 2:
                raise ValueError("Weibull distribution requires exactly 2 parameter: shape, scale")
            shape, scale = params
            return lambda: safe_time(rng.weibull(shape) * scale)

        raise ValueError(f"Unknown/unsupported distribution type: {time_distribution_type}")
