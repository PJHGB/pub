"""
BMW TDV6 Injector Balance Rates sensor module.

Balance rates show how much the ECU trims each injector to equalise
cylinder contribution. Large positive balance = injector under-delivering.
Large negative balance = injector over-delivering.
The TDV6 uses Siemens piezo injectors.
Common faults:
  P0201–P0206 - Injector circuit malfunction cylinders 1–6
  P0261–P0272 - Injector circuit low/high per cylinder
  P1141–P1146 - Injector balance rate out of range per cylinder (BMW-specific)
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

CYLINDER_COUNT = 6
BALANCE_RATE_WARN_MG = 3.0    # mg/stroke deviation from zero
BALANCE_RATE_CRITICAL_MG = 6.0

INJECTOR_CIRCUIT_CODES = {
    i: f'P020{i}' for i in range(1, 7)
}
BALANCE_RATE_CODES = {
    1: 'P1141', 2: 'P1142', 3: 'P1143',
    4: 'P1144', 5: 'P1145', 6: 'P1146',
}


class InjectorSensor(BaseSensor):
    name = 'injectors'
    unit = 'mg/stroke'

    def read(self) -> SensorReading:
        balance_rates = {
            f'cylinder_{i}': round(random.uniform(-8, 8), 2)
            for i in range(1, CYLINDER_COUNT + 1)
        }
        injection_quantity_mg = round(random.uniform(5, 60), 1)
        pilot_injection_active = random.choice([True, False])

        fault_codes = []
        critical_cyls = []
        warn_cyls = []

        for i in range(1, CYLINDER_COUNT + 1):
            rate = balance_rates[f'cylinder_{i}']
            abs_rate = abs(rate)
            if abs_rate >= BALANCE_RATE_CRITICAL_MG:
                critical_cyls.append(i)
                direction = 'over-delivering' if rate < 0 else 'under-delivering'
                fault_codes.append(FaultCode(
                    BALANCE_RATE_CODES[i],
                    f'Injector cylinder {i} balance rate critical ({rate:+.2f} mg) — {direction}',
                    'critical',
                ))
            elif abs_rate >= BALANCE_RATE_WARN_MG:
                warn_cyls.append(i)
                fault_codes.append(FaultCode(
                    BALANCE_RATE_CODES[i],
                    f'Injector cylinder {i} balance rate elevated ({rate:+.2f} mg)',
                    'warning',
                ))

        if critical_cyls:
            status = 'critical'
        elif warn_cyls:
            status = 'warning'
        else:
            status = 'ok'

        avg_balance = round(sum(balance_rates.values()) / CYLINDER_COUNT, 2)

        return self._make_reading(
            value=avg_balance,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'balance_rates_mg': balance_rates,
                'injection_quantity_mg': injection_quantity_mg,
                'pilot_injection_active': pilot_injection_active,
                'critical_cylinders': critical_cyls,
                'warn_cylinders': warn_cyls,
            },
        )
