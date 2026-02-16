"""
BMW TDV6 Coolant Temperature sensor module.

Normal operating range: 85–105°C (thermostat opens ~88°C)
  P0115 - Coolant temperature circuit malfunction
  P0116 - Coolant temperature circuit range/performance
  P0117 - Coolant temperature circuit low input
  P0118 - Coolant temperature circuit high input
  P0128 - Coolant temperature below thermostat regulating temperature
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

COOLANT_LOW_C = 75
COOLANT_WARN_C = 110
COOLANT_CRITICAL_C = 120
COOLANT_THERMOSTAT_C = 88


class CoolantTemperatureSensor(BaseSensor):
    name = 'coolant_temperature'
    unit = '°C'

    def read(self) -> SensorReading:
        temp = round(random.uniform(50, 125), 1)
        fault_codes = []
        status = 'ok'

        if temp < COOLANT_LOW_C:
            status = 'warning'
            fault_codes.append(FaultCode('P0128', 'Coolant temperature below thermostat regulating temp', 'warning'))
        elif temp >= COOLANT_CRITICAL_C:
            status = 'critical'
            fault_codes.append(FaultCode('P0118', 'Coolant temperature critically high — check cooling system', 'critical'))
        elif temp >= COOLANT_WARN_C:
            status = 'warning'
            fault_codes.append(FaultCode('P0116', 'Coolant temperature elevated', 'warning'))

        return self._make_reading(
            value=temp,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'thermostat_opens_c': COOLANT_THERMOSTAT_C,
                'normal_range_c': [85, 105],
            },
        )
