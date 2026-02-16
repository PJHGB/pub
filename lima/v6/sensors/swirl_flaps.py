"""
BMW TDV6 Swirl Flap sensor module.

Swirl flaps (intake manifold flaps) improve combustion at low RPM.
NOTORIOUS failure point on the TDV6/M57 — flap spindles break and
fall into the engine, causing catastrophic damage.
Common faults:
  P1529 - Intake manifold flap position sensor malfunction
  P1530 - Intake manifold flap stuck
  P2004 - Intake manifold runner control stuck open (bank 1)
  P2005 - Intake manifold runner control stuck open (bank 2)
  P2006 - Intake manifold runner control stuck closed (bank 1)
  P2007 - Intake manifold runner control stuck closed (bank 2)
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

FLAP_BANKS = ['bank_1', 'bank_2']
FLAP_STUCK_THRESHOLD_PCT = 5     # deviation < threshold when commanded to move = stuck
SPINDLE_TORQUE_WARN_NM = 0.8    # elevated torque may indicate spindle seizure


class SwirlFlapSensor(BaseSensor):
    name = 'swirl_flaps'
    unit = '%'

    def read(self) -> SensorReading:
        banks = {}
        for bank in FLAP_BANKS:
            commanded_pct = round(random.uniform(0, 100), 1)
            actual_pct = round(commanded_pct + random.uniform(-15, 15), 1)
            actual_pct = max(0.0, min(100.0, actual_pct))
            spindle_torque_nm = round(random.uniform(0.1, 1.5), 3)
            banks[bank] = {
                'commanded_pct': commanded_pct,
                'actual_pct': actual_pct,
                'deviation_pct': round(abs(commanded_pct - actual_pct), 1),
                'spindle_torque_nm': spindle_torque_nm,
            }

        fault_codes = []
        status = 'ok'

        for bank, data in banks.items():
            bank_num = '1' if bank == 'bank_1' else '2'
            deviation = data['deviation_pct']
            torque = data['spindle_torque_nm']

            if deviation > 20:
                status = 'critical'
                stuck_open = data['actual_pct'] > 50
                code = f'P200{"4" if stuck_open else "6"}' if bank == 'bank_1' else f'P200{"5" if stuck_open else "7"}'
                fault_codes.append(FaultCode(
                    code,
                    f'Swirl flap {bank} stuck {"open" if stuck_open else "closed"} — spindle failure risk',
                    'critical',
                ))
            elif deviation > FLAP_STUCK_THRESHOLD_PCT:
                if status != 'critical':
                    status = 'warning'
                fault_codes.append(FaultCode(
                    'P1530',
                    f'Swirl flap {bank} position deviation {deviation:.1f}% from commanded',
                    'warning',
                ))

            if torque >= SPINDLE_TORQUE_WARN_NM:
                fault_codes.append(FaultCode(
                    'P1529',
                    f'Swirl flap {bank} spindle torque elevated ({torque:.3f} Nm) — spindle seizure warning',
                    'warning',
                ))
                if status != 'critical':
                    status = 'warning'

        avg_position = round(
            sum(b['actual_pct'] for b in banks.values()) / len(FLAP_BANKS), 1
        )

        return self._make_reading(
            value=avg_position,
            status=status,
            fault_codes=fault_codes,
            metadata={'banks': banks},
        )
