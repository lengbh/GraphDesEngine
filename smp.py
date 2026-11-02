"""Local semi-Markov models, independent of simulation and MES control."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import math

from random_util import RandomFactory


@dataclass(frozen=True)
class SMPTransition:
    """A sampled transition; ``None`` holding time means external control.

    A timed transition leaves ``current_state`` after ``holding_time``.
    An externally controlled transition waits for the caller's enabling event,
    such as an MES execute decision. ``None`` must not be treated as zero time.
    """

    current_state: str
    next_state: str
    holding_time: float | None


class SMP:
    """States, transition probabilities P, and conditional holding times F.

    Holding-time samplers are indexed by (current_state, next_state). A sampler
    of ``None`` represents an externally controlled transition, rather than a
    random holding time. This supports an idle resource waiting for MES control.

    Sampling does not advance time or change the resource's current state.
    The simulation schedules execution using its existing SimPy clock, while
    MES retains its action, routing, and release decisions.
    """

    def __init__(
        self,
        states: Sequence[str],
        transition_probabilities: Mapping[str, Mapping[str, float]],
        holding_time_samplers: Mapping[tuple[str, str], Callable[[], float] | None],
        initial_state: str,
    ):
        self.states = tuple(states)
        if not self.states or any(not isinstance(state, str) or not state for state in self.states):
            raise ValueError("SMP states must be nonempty strings")
        if len(set(self.states)) != len(self.states):
            raise ValueError("SMP states must be unique")
        if initial_state not in self.states:
            raise ValueError(f"Unknown initial SMP state: {initial_state!r}")
        if set(transition_probabilities) != set(self.states):
            raise ValueError("Transition probabilities must provide a row for every SMP state")

        self.initial_state = initial_state
        self._transitions: dict[str, tuple[tuple[str, ...], tuple[float, ...]]] = {}
        self._holding_time_samplers = dict(holding_time_samplers)

        for state in self.states:
            row = transition_probabilities[state]
            if any(target not in self.states for target in row):
                raise ValueError(f"Unknown transition target in SMP state {state!r}")
            probabilities = {target: float(probability) for target, probability in row.items()}
            if any(not math.isfinite(probability) or probability < 0 for probability in probabilities.values()):
                raise ValueError(f"Invalid transition probability in SMP state {state!r}")
            if not math.isclose(sum(probabilities.values()), 1.0, rel_tol=0.0, abs_tol=1e-12):
                raise ValueError(f"Transition probabilities for SMP state {state!r} must sum to 1")

            targets = tuple(target for target, probability in probabilities.items() if probability > 0)
            self._transitions[state] = (targets, tuple(probabilities[target] for target in targets))
            for target in targets:
                key = (state, target)
                if key not in self._holding_time_samplers:
                    raise ValueError(f"Missing holding-time sampler for SMP transition {key!r}")
                sampler = self._holding_time_samplers[key]
                if sampler is not None and not callable(sampler):
                    raise ValueError(f"Holding-time sampler for SMP transition {key!r} must be callable or None")

        for key in self._holding_time_samplers:
            if not isinstance(key, tuple) or len(key) != 2:
                raise ValueError("Holding-time sampler keys must be (current_state, next_state) pairs")
            state, target = key
            if state not in self._transitions or target not in self._transitions[state][0]:
                raise ValueError(f"Holding-time sampler has no positive-probability SMP transition: {key!r}")

    @classmethod
    def from_json(cls, model: Mapping) -> 'SMP':
        """Load P and F from JSON; a null distribution means external control."""
        required = ("states", "initial_state", "transition_probabilities", "holding_time_distributions")
        if not isinstance(model, Mapping) or any(key not in model for key in required):
            raise ValueError("An SMP definition requires states, initial_state, transition_probabilities, and holding_time_distributions")
        distributions = model["holding_time_distributions"]
        if not isinstance(distributions, Mapping):
            raise ValueError("SMP holding_time_distributions must be indexed by current and next state")
        samplers = {}
        for state, row in distributions.items():
            if not isinstance(row, Mapping):
                raise ValueError(f"Invalid holding-time distribution row for SMP state {state!r}")
            for target, distribution in row.items():
                if distribution is None:
                    samplers[(state, target)] = None
                else:
                    if not isinstance(distribution, Mapping) or not {"type", "parameters"} <= distribution.keys():
                        raise ValueError(f"Invalid holding-time distribution for SMP transition {(state, target)!r}")
                    samplers[(state, target)] = RandomFactory.get_random_generator(
                        distribution["type"], distribution["parameters"],
                    )
        return cls(model["states"], model["transition_probabilities"], samplers, model["initial_state"])

    def require_ideal_machine(self):
        """Reject models whose execution semantics the current station runtime lacks."""
        if (set(self.states) != {"idle", "running"} or self.initial_state != "idle"
                or self._transitions["idle"][0] != ("running",)
                or self._transitions["running"][0] != ("idle",)
                or self._holding_time_samplers[("idle", "running")] is not None
                or self._holding_time_samplers[("running", "idle")] is None):
            raise ValueError("Station SMP must start idle, enter running under external control, and return to idle after a sampled holding time")

    def get_holding_time_sampler(self, current_state: str, next_state: str) -> Callable[[], float] | None:
        """Expose a conditional sampler for compatibility with existing graph APIs."""
        try:
            return self._holding_time_samplers[(current_state, next_state)]
        except KeyError as error:
            raise ValueError(f"Unknown SMP transition: {(current_state, next_state)!r}") from error

    @classmethod
    def ideal_machine(cls, running_time_sampler: Callable[[], float]) -> 'SMP':
        """Wrap an existing service-time sampler in an idle/running model.

        MES enables idle -> running. Running -> idle has probability 1 and
        uses the supplied service-time distribution. Neither transition needs
        an additional random draw to select its next state.
        """
        if not callable(running_time_sampler):
            raise ValueError("The running-time sampler must be callable")
        return cls(
            states=("idle", "running"),
            transition_probabilities={"idle": {"running": 1.0}, "running": {"idle": 1.0}},
            holding_time_samplers={
                ("idle", "running"): None,
                ("running", "idle"): running_time_sampler,
            },
            initial_state="idle",
        )

    def sample_transition(self, current_state: str) -> SMPTransition:
        """Sample j from P[i], then the holding time in i from F[i, j].

        The resource remains in ``current_state`` until the sampled time has
        elapsed, or until its external enabling event when the duration is None.
        """
        if current_state not in self._transitions:
            raise ValueError(f"Unknown SMP state: {current_state!r}")
        targets, probabilities = self._transitions[current_state]
        if len(targets) == 1:
            next_state = targets[0]
        else:
            index = int(RandomFactory._get_rng().choice(len(targets), p=probabilities))
            next_state = targets[index]

        sampler = self._holding_time_samplers[(current_state, next_state)]
        holding_time = None if sampler is None else float(sampler())
        if holding_time is not None and (not math.isfinite(holding_time) or holding_time < 0):
            raise ValueError("Sampled SMP holding times must be finite and nonnegative")
        return SMPTransition(current_state, next_state, holding_time)
