"""
BMW TDV6 Oil Temperature sensor module.

Normal operating range: 80–130°C
  P0196 - Oil temperature sensor range/performance
  P0197 - Oil temperature too low
  P0198 - Oil temperature too high
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

OIL_TEMP_LOW_C = 60
OIL_TEMP_WARN_C = 130
OIL_TEMP_CRITICAL_C = 150


class OilTemperatureSensor(BaseSensor):
    name = 'oil_temperature'
    unit = '°C'

    def read(self) -> SensorReading:
        temp = round(random.uniform(55, 155), 1)
        fault_codes = []
        status = 'ok'

        if temp < OIL_TEMP_LOW_C:
            status = 'warning'
            fault_codes.append(FaultCode('P0197', 'Oil temperature too low — engine not at operating temp', 'warning'))
        elif temp >= OIL_TEMP_CRITICAL_C:
            status = 'critical'
            fault_codes.append(FaultCode('P0198', 'Oil temperature critically high — risk of engine damage', 'critical'))
        elif temp >= OIL_TEMP_WARN_C:
            status = 'warning'
            fault_codes.append(FaultCode('P0196', 'Oil temperature elevated', 'warning'))

        return self._make_reading(
            value=temp,
            status=status,
            fault_codes=fault_codes,
            metadata={'normal_range_c': [80, 130]},
        )
