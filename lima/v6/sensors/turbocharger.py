"""
BMW TDV6 Turbocharger sensor module.

The TDV6 uses a variable-geometry turbocharger (VGT). Common faults:
  P0299 - Underboost condition
  P0234 - Overboost condition
  P2563 - Turbo boost control position sensor range/performance
  P0045 - Turbo/supercharger boost control solenoid circuit open
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

# BMW TDV6 boost pressure thresholds (bar absolute)
BOOST_MIN_BAR = 1.4   # minimum expected under load
BOOST_MAX_BAR = 2.6   # maximum safe boost
BOOST_OVERBOOST_BAR = 2.8

# VGT vane position range (0-100%)
VANE_MIN_PCT = 5
VANE_MAX_PCT = 95


class TurbochargerSensor(BaseSensor):
    name = 'turbocharger'
    unit = 'bar'

    def read(self) -> SensorReading:
        boost_bar = round(random.uniform(1.2, 2.7), 2)
        vane_position_pct = round(random.uniform(10, 90), 1)
        fault_codes = []
        status = 'ok'

        if boost_bar < BOOST_MIN_BAR:
            status = 'warning'
            fault_codes.append(FaultCode('P0299', 'Turbocharger underboost condition', 'warning'))
        elif boost_bar > BOOST_OVERBOOST_BAR:
            status = 'critical'
            fault_codes.append(FaultCode('P0234', 'Turbocharger overboost condition', 'critical'))
        elif boost_bar > BOOST_MAX_BAR:
            status = 'warning'

        if not (VANE_MIN_PCT < vane_position_pct < VANE_MAX_PCT):
            fault_codes.append(FaultCode('P2563', 'VGT vane position out of range', 'warning'))
            status = max(status, 'warning')

        return self._make_reading(
            value=boost_bar,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'vane_position_pct': vane_position_pct,
                'boost_target_bar': 2.1,
            },
        )
