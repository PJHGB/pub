"""
BMW TDV6 Exhaust Gas Recirculation (EGR) sensor module.

EGR reduces NOx emissions by recirculating exhaust back into intake.
Common faults on TDV6:
  P0400 - EGR flow malfunction
  P0401 - EGR insufficient flow detected
  P0402 - EGR excessive flow detected
  P0403 - EGR control circuit malfunction
  P0404 - EGR control circuit range/performance
  P0405 - EGR position sensor circuit low
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

EGR_VALVE_STUCK_THRESHOLD_PCT = 5    # valve appears stuck if position delta < 5%
EGR_FLOW_LOW_KGH = 0.5
EGR_FLOW_HIGH_KGH = 8.0


class EGRSensor(BaseSensor):
    name = 'egr'
    unit = 'kg/h'

    def read(self) -> SensorReading:
        flow_rate_kgh = round(random.uniform(0, 9), 2)
        valve_position_pct = round(random.uniform(0, 100), 1)
        valve_target_pct = round(random.uniform(0, 100), 1)
        cooler_temp_c = round(random.uniform(40, 180), 1)
        delta = abs(valve_position_pct - valve_target_pct)

        fault_codes = []
        status = 'ok'

        if flow_rate_kgh < EGR_FLOW_LOW_KGH and valve_position_pct > 20:
            status = 'warning'
            fault_codes.append(FaultCode('P0401', 'EGR insufficient flow — valve may be coked/stuck', 'warning'))
        elif flow_rate_kgh > EGR_FLOW_HIGH_KGH:
            status = 'warning'
            fault_codes.append(FaultCode('P0402', 'EGR excessive flow detected', 'warning'))

        if delta > 25:
            if status != 'critical':
                status = 'warning'
            fault_codes.append(FaultCode('P0404', 'EGR valve position deviation from target', 'warning'))

        return self._make_reading(
            value=flow_rate_kgh,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'valve_position_pct': valve_position_pct,
                'valve_target_pct': valve_target_pct,
                'cooler_temp_c': cooler_temp_c,
            },
        )
