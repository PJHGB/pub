"""
BMW TDV6 Boost / MAP (Manifold Absolute Pressure) sensor module.

Measures actual intake manifold pressure vs. turbo target.
Complements turbocharger sensor with downstream manifold readings.
Common faults:
  P0105 - MAP circuit malfunction
  P0106 - MAP circuit range/performance
  P0107 - MAP circuit low input
  P0108 - MAP circuit high input
  P0236 - Turbocharger boost sensor A circuit range/performance
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

# Manifold absolute pressure (bar absolute)
MAP_IDLE_MIN_BAR = 0.9
MAP_IDLE_MAX_BAR = 1.1
MAP_BOOST_MAX_BAR = 2.8
MAP_CRITICAL_HIGH_BAR = 3.0

BOOST_DEVIATION_WARN_BAR = 0.3   # actual vs target deviation threshold


class BoostPressureSensor(BaseSensor):
    name = 'boost_pressure'
    unit = 'bar'

    def read(self) -> SensorReading:
        actual_bar = round(random.uniform(0.8, 3.1), 3)
        target_bar = round(random.uniform(1.0, 2.8), 3)
        deviation = round(abs(actual_bar - target_bar), 3)
        charge_air_temp_c = round(random.uniform(25, 75), 1)

        fault_codes = []
        status = 'ok'

        if actual_bar >= MAP_CRITICAL_HIGH_BAR:
            status = 'critical'
            fault_codes.append(FaultCode('P0108', 'MAP pressure critically high', 'critical'))
        elif actual_bar > MAP_BOOST_MAX_BAR:
            status = 'warning'
            fault_codes.append(FaultCode('P0106', 'MAP pressure above maximum boost spec', 'warning'))
        elif actual_bar < MAP_IDLE_MIN_BAR:
            status = 'warning'
            fault_codes.append(FaultCode('P0107', 'MAP pressure below idle minimum — vacuum leak possible', 'warning'))

        if deviation > BOOST_DEVIATION_WARN_BAR:
            if status != 'critical':
                status = 'warning'
            fault_codes.append(FaultCode('P0236', f'Boost pressure deviation {deviation:.3f} bar from target', 'warning'))

        return self._make_reading(
            value=actual_bar,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'target_bar': target_bar,
                'deviation_bar': deviation,
                'charge_air_temp_c': charge_air_temp_c,
            },
        )
