"""
BMW TDV6 Mass Air Flow (MAF) sensor module.

The MAF measures intake air mass to calculate fuelling.
At idle: ~20–30 g/s; under full load: 250–400 g/s.
Common faults:
  P0100 - MAF circuit malfunction
  P0101 - MAF circuit range/performance
  P0102 - MAF circuit low input (clogged/failed sensor)
  P0103 - MAF circuit high input
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

MAF_IDLE_MIN_GS = 15
MAF_IDLE_MAX_GS = 40
MAF_LOAD_MAX_GS = 420
MAF_CRITICAL_LOW_GS = 5
MAF_CRITICAL_HIGH_GS = 450

# Intake air temperature normal range
IAT_WARN_C = 60
IAT_CRITICAL_C = 80


class MAFSensor(BaseSensor):
    name = 'maf'
    unit = 'g/s'

    def read(self) -> SensorReading:
        maf_gs = round(random.uniform(3, 460), 1)
        intake_air_temp_c = round(random.uniform(15, 85), 1)
        air_density = round(1.225 * (273.15 / (273.15 + intake_air_temp_c)), 3)

        fault_codes = []
        status = 'ok'

        if maf_gs < MAF_CRITICAL_LOW_GS:
            status = 'critical'
            fault_codes.append(FaultCode('P0102', 'MAF reading critically low — sensor may be failed or disconnected', 'critical'))
        elif maf_gs > MAF_CRITICAL_HIGH_GS:
            status = 'critical'
            fault_codes.append(FaultCode('P0103', 'MAF reading critically high — sensor fault', 'critical'))
        elif maf_gs < MAF_IDLE_MIN_GS:
            status = 'warning'
            fault_codes.append(FaultCode('P0101', 'MAF reading below idle minimum — check for air leaks or dirty sensor', 'warning'))
        elif maf_gs > MAF_LOAD_MAX_GS:
            status = 'warning'
            fault_codes.append(FaultCode('P0103', 'MAF reading above maximum load value', 'warning'))

        if intake_air_temp_c >= IAT_CRITICAL_C:
            if status != 'critical':
                status = 'warning'
            fault_codes.append(FaultCode('P0113', 'Intake air temperature critically high — check intercooler', 'warning'))

        return self._make_reading(
            value=maf_gs,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'intake_air_temp_c': intake_air_temp_c,
                'air_density_kg_m3': air_density,
            },
        )
