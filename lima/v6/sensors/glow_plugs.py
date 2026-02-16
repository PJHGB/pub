"""
BMW TDV6 Glow Plug sensor module.

The TDV6 has 6 cylinders, each with a glow plug for cold-start combustion.
Glow plug resistance indicates health — high resistance = failed plug.
Common faults:
  P0670 - Glow plug control module circuit
  P0671–P0676 - Glow plug circuit open/short cylinders 1–6
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

CYLINDER_COUNT = 6
GLOW_PLUG_NOMINAL_OHMS = 0.5
GLOW_PLUG_WARN_OHMS = 2.0
GLOW_PLUG_FAILED_OHMS = 5.0

FAULT_CODES_BY_CYLINDER = {
    1: 'P0671', 2: 'P0672', 3: 'P0673',
    4: 'P0674', 5: 'P0675', 6: 'P0676',
}


class GlowPlugSensor(BaseSensor):
    name = 'glow_plugs'
    unit = 'Ω'

    def read(self) -> SensorReading:
        resistances = {
            f'cylinder_{i}': round(random.uniform(0.3, 6.0), 2)
            for i in range(1, CYLINDER_COUNT + 1)
        }
        fault_codes = []
        failed_cylinders = []
        warn_cylinders = []

        for i in range(1, CYLINDER_COUNT + 1):
            r = resistances[f'cylinder_{i}']
            if r >= GLOW_PLUG_FAILED_OHMS:
                failed_cylinders.append(i)
                fault_codes.append(FaultCode(
                    FAULT_CODES_BY_CYLINDER[i],
                    f'Glow plug cylinder {i} failed (resistance {r:.2f} Ω)',
                    'critical',
                ))
            elif r >= GLOW_PLUG_WARN_OHMS:
                warn_cylinders.append(i)
                fault_codes.append(FaultCode(
                    FAULT_CODES_BY_CYLINDER[i],
                    f'Glow plug cylinder {i} degraded (resistance {r:.2f} Ω)',
                    'warning',
                ))

        if failed_cylinders:
            status = 'critical'
        elif warn_cylinders:
            status = 'warning'
        else:
            status = 'ok'

        avg_resistance = round(sum(resistances.values()) / CYLINDER_COUNT, 2)

        return self._make_reading(
            value=avg_resistance,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'resistances_ohm': resistances,
                'failed_cylinders': failed_cylinders,
                'warn_cylinders': warn_cylinders,
                'nominal_resistance_ohm': GLOW_PLUG_NOMINAL_OHMS,
            },
        )
