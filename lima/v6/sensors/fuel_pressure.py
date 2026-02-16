"""
BMW TDV6 Fuel Pressure sensor module.

The TDV6 common-rail system runs 300–1800 bar rail pressure.
The Siemens/Continental high-pressure pump (CP4) is a known weak point.
Common faults:
  P0087 - Fuel rail/system pressure too low
  P0088 - Fuel rail/system pressure too high
  P0190 - Fuel rail pressure sensor circuit malfunction
  P0191 - Fuel rail pressure sensor range/performance
  P1093 - Fuel rail pressure too low during regeneration
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

RAIL_PRESSURE_MIN_BAR = 300
RAIL_PRESSURE_IDLE_BAR = 350
RAIL_PRESSURE_MAX_BAR = 1800
RAIL_PRESSURE_CRITICAL_LOW_BAR = 200
RAIL_PRESSURE_CRITICAL_HIGH_BAR = 1900

LOW_PRESSURE_CIRCUIT_MIN_BAR = 4.5
LOW_PRESSURE_CIRCUIT_MAX_BAR = 6.5


class FuelPressureSensor(BaseSensor):
    name = 'fuel_pressure'
    unit = 'bar'

    def read(self) -> SensorReading:
        rail_pressure_bar = round(random.uniform(180, 1950), 0)
        low_pressure_bar = round(random.uniform(3.5, 7.0), 2)
        pump_control_pct = round(random.uniform(20, 95), 1)

        fault_codes = []
        status = 'ok'

        if rail_pressure_bar <= RAIL_PRESSURE_CRITICAL_LOW_BAR:
            status = 'critical'
            fault_codes.append(FaultCode('P0087', 'Fuel rail pressure critically low — HP pump failure likely', 'critical'))
        elif rail_pressure_bar < RAIL_PRESSURE_MIN_BAR:
            status = 'warning'
            fault_codes.append(FaultCode('P0087', 'Fuel rail pressure low', 'warning'))
        elif rail_pressure_bar >= RAIL_PRESSURE_CRITICAL_HIGH_BAR:
            status = 'critical'
            fault_codes.append(FaultCode('P0088', 'Fuel rail pressure critically high — pressure relief risk', 'critical'))
        elif rail_pressure_bar > RAIL_PRESSURE_MAX_BAR:
            status = 'warning'
            fault_codes.append(FaultCode('P0088', 'Fuel rail pressure above maximum', 'warning'))

        if low_pressure_bar < LOW_PRESSURE_CIRCUIT_MIN_BAR:
            if status != 'critical':
                status = 'warning'
            fault_codes.append(FaultCode('P0190', 'Low pressure circuit below spec — check lift pump / filter', 'warning'))

        return self._make_reading(
            value=rail_pressure_bar,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'low_pressure_circuit_bar': low_pressure_bar,
                'pump_control_pct': pump_control_pct,
                'normal_rail_range_bar': [RAIL_PRESSURE_MIN_BAR, RAIL_PRESSURE_MAX_BAR],
            },
        )
