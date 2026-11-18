"""Conveyor travel driven by local SMPs without changing MES release rules.

Each in-flight transfer has its own idle/running SMP.  Transfers on one arc
may overlap, as they did with the original independently sampled delays.
An optional, shared equipment-condition SMP changes the speed of every
in-flight transfer on calendar time, including periods without any trays.
"""

from dataclasses import dataclass
from typing import Any, Callable

import simpy

from labelled_graph import Arc


@dataclass
class ConveyorTransfer:
    id: str
    tray_id: int
    workpiece_id: int
    nominal_time: float
    started_at: float
    arrived_at: float | None = None
    remaining_work: float = 0.0
    segment_started_at: float = 0.0
    segment_speed: float = 1.0


class ConveyorRuntime:
    """Physical travel on an arc, independent of downstream buffer admission."""

    def __init__(self, env: simpy.Environment, arc: Arc,
                 emit_event: Callable[[dict[str, Any]], None]):
        self.env = env
        self.arc = arc
        self.emit_event = emit_event
        self.transfer_number = 0
        self.active_transfers: dict[str, ConveyorTransfer] = {}
        self.condition_smp = arc.condition_smp
        self.condition_state = self.condition_smp.initial_state if self.condition_smp else 'nominal'
        self.condition_entered_at = float(env.now)
        self._condition_changed = env.event()
        self.condition_process = env.process(self._run_condition()) if self.condition_smp else None

    @property
    def speed_factor(self) -> float:
        return self.arc.condition_speed_factors[self.condition_state]

    def _run_condition(self):
        assert self.condition_smp is not None
        while True:
            transition = self.condition_smp.sample_transition(self.condition_state)
            if transition.holding_time is None or transition.holding_time <= 0:
                raise ValueError('Conveyor condition holding times must be strictly positive')
            yield self.env.timeout(transition.holding_time)
            previous, entered = self.condition_state, self.condition_entered_at
            self.condition_state = transition.next_state
            self.condition_entered_at = float(self.env.now)
            self.emit_event({
                'type': 'conveyor_condition_transition', 't': self.env.now,
                'tail': self.arc.tail, 'head': self.arc.head,
                'state_from': previous, 'state_to': self.condition_state,
                'state_entered_at': entered, 'holding_time': float(self.env.now) - entered,
                'speed_factor': self.speed_factor,
            })
            previous_event = self._condition_changed
            self._condition_changed = self.env.event()
            previous_event.succeed()

    def sample_transfer_time(self) -> float:
        """Sample the ideal running hold at the original dispatch-time point."""
        start = self.arc.smp.sample_transition('idle')
        if start.next_state != 'running' or start.holding_time is not None:
            raise RuntimeError('Conveyor travel must start under external control')
        completion = self.arc.smp.sample_transition('running')
        if completion.next_state != 'idle' or completion.holding_time is None:
            raise RuntimeError('Conveyor running must end after a timed transition')
        return completion.holding_time

    def begin_transfer(self, tray_id: int, workpiece_id: int, nominal_time: float) -> ConveyorTransfer:
        self.transfer_number += 1
        transfer = ConveyorTransfer(
            f'A{self.arc.tail}_{self.arc.head}-{self.transfer_number}',
            tray_id, workpiece_id, nominal_time, float(self.env.now),
            remaining_work=nominal_time, segment_started_at=float(self.env.now),
            segment_speed=self.speed_factor,
        )
        self.active_transfers[transfer.id] = transfer
        self._emit(transfer, 'transfer_start', transfer_time=nominal_time)
        self._emit(transfer, 'transfer_smp_transition', state_from='idle', state_to='running',
                   state_entered_at=self.env.now, holding_time=0.0)
        return transfer

    def travel(self, transfer: ConveyorTransfer):
        if self.condition_smp is None:
            # Yield directly in the existing transfer process: no extra process,
            # random draw, or service resource changes legacy event scheduling.
            yield self.env.timeout(transfer.nominal_time)
            transfer.remaining_work = 0.0
        elif transfer.nominal_time == 0:
            yield self.env.timeout(0)
        else:
            yield from self._travel_with_condition(transfer)
        transfer.arrived_at = float(self.env.now)
        self.active_transfers.pop(transfer.id)
        self._emit(transfer, 'transfer_smp_transition', state_from='running', state_to='idle',
                   state_entered_at=transfer.started_at,
                   holding_time=transfer.arrived_at - transfer.started_at)
        self._emit(transfer, 'transfer_arrival', travel_time=transfer.arrived_at - transfer.started_at)

    def _travel_with_condition(self, transfer: ConveyorTransfer):
        # Nominal transfer seconds are work units: d(work)/dt = speed_factor.
        # Each shared state change wakes all transfers without redrawing work.
        while transfer.remaining_work > 0:
            speed = self.speed_factor
            condition = self.condition_state
            transfer.segment_started_at = float(self.env.now)
            transfer.segment_speed = speed
            changed = self._condition_changed
            completed = False
            if speed == 0:
                yield changed
            else:
                completion = self.env.timeout(transfer.remaining_work / speed)
                outcome = yield completion | changed
                completed = completion in outcome
            elapsed = float(self.env.now) - transfer.segment_started_at
            # A processed completion consumes all scheduled work. Recomputing
            # that fact from rounded absolute times can leave a positive
            # residual whose next timeout rounds back to the same clock value.
            progress = (transfer.remaining_work if completed else
                        min(transfer.remaining_work, elapsed * speed))
            transfer.remaining_work = max(0.0, transfer.remaining_work - progress)
            self._emit(transfer, 'transfer_progress', speed_factor=speed,
                       progress=progress, remaining_work=transfer.remaining_work,
                       condition_state=condition)

    def observe(self, activity: str):
        self.emit_event({
            'type': f'conveyor_condition_{activity}', 't': self.env.now,
            'tail': self.arc.tail, 'head': self.arc.head,
            'state_to': self.condition_state, 'state_entered_at': self.condition_entered_at,
            'holding_time': float(self.env.now) - self.condition_entered_at,
            'speed_factor': self.speed_factor,
        })
        if activity == 'censored':
            for transfer in self.active_transfers.values():
                remaining = max(0.0, transfer.remaining_work - (
                    float(self.env.now) - transfer.segment_started_at) * transfer.segment_speed)
                self._emit(transfer, 'transfer_smp_censored', state_to='running',
                           state_entered_at=transfer.started_at,
                           holding_time=float(self.env.now) - transfer.started_at,
                           remaining_work=remaining, condition_state=self.condition_state)

    def finish_admission(self, transfer: ConveyorTransfer, workpiece_id: int | None = None):
        if transfer.arrived_at is None:
            raise RuntimeError('A transfer cannot enter the next station before physical arrival')
        self._emit(transfer, 'transfer_end', travel_time=transfer.arrived_at - transfer.started_at,
                   admission_wait=float(self.env.now) - transfer.arrived_at,
                   workpiece_id=transfer.workpiece_id if workpiece_id is None else workpiece_id)

    def _emit(self, transfer: ConveyorTransfer, activity: str, **fields: Any):
        self.emit_event({
            'type': activity, 't': self.env.now, 'tail': self.arc.tail, 'head': self.arc.head,
            'tray_id': transfer.tray_id, 'workpiece_id': transfer.workpiece_id,
            'transfer_id': transfer.id, **fields,
        })
