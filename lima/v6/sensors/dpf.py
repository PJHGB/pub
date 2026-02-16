"""
BMW TDV6 Diesel Particulate Filter (DPF) sensor module.

Monitors soot load %, differential pressure (backpressure), and regen state.
Common faults:
  P2002 - DPF efficiency below threshold
  P2452 - DPF differential pressure sensor circuit
  P2453 - DPF differential pressure sensor range/performance
  P244A - DPF restriction — ash accumulation level too high
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

DPF_SOOT_WARN_PCT = 70
DPF_SOOT_REGEN_PCT = 80    # forced regen threshold
DPF_SOOT_CRITICAL_PCT = 95
DPF_BACKPRESSURE_WARN_MBAR = 80
DPF_BACKPRESSURE_CRITICAL_MBAR = 120

REGEN_STATES = ['idle', 'passive', 'active', 'forced']


class DPFSensor(BaseSensor):
    name = 'dpf'
    unit = '%'

    def read(self) -> SensorReading:
        soot_pct = round(random.uniform(10, 98), 1)
        backpressure_mbar = round(random.uniform(20, 130), 1)
        regen_state = random.choice(REGEN_STATES)
        ash_level_pct = round(random.uniform(0, 60), 1)

        fault_codes = []
        status = 'ok'

        if soot_pct >= DPF_SOOT_CRITICAL_PCT:
            status = 'critical'
            fault_codes.append(FaultCode('P2002', 'DPF soot level critical — immediate regeneration required', 'critical'))
        elif soot_pct >= DPF_SOOT_REGEN_PCT:
            status = 'warning'
            fault_codes.append(FaultCode('P2002', 'DPF soot load high — regeneration needed', 'warning'))

        if backpressure_mbar >= DPF_BACKPRESSURE_CRITICAL_MBAR:
            status = 'critical'
            fault_codes.append(FaultCode('P2453', 'DPF differential pressure critically high', 'critical'))
        elif backpressure_mbar >= DPF_BACKPRESSURE_WARN_MBAR:
            if status != 'critical':
                status = 'warning'
            fault_codes.append(FaultCode('P2452', 'DPF differential pressure elevated', 'warning'))

        if ash_level_pct > 45:
            fault_codes.append(FaultCode('P244A', 'DPF ash accumulation level high — replacement may be needed', 'warning'))

        return self._make_reading(
            value=soot_pct,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'backpressure_mbar': backpressure_mbar,
                'regen_state': regen_state,
                'ash_level_pct': ash_level_pct,
            },
        )
