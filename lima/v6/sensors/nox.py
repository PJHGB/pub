"""
BMW TDV6 NOx / Lambda sensor module.

Post-DPF NOx sensor monitors emissions compliance and SCR catalyst (if fitted).
Lambda (O2) sensor measures exhaust oxygen content.
Common faults:
  P0130 - O2 sensor circuit malfunction (bank 1 sensor 1)
  P0136 - O2 sensor circuit malfunction (bank 1 sensor 2)
  P2200 - NOx sensor circuit (bank 1)
  P2201 - NOx sensor circuit range/performance (bank 1)
  P229F - NOx sensor (bank 2) — downstream
"""
import random
from .base import BaseSensor, FaultCode, SensorReading

# NOx thresholds (ppm) — Euro 5: 180 ppm, Euro 6: 80 ppm
NOX_EURO5_LIMIT_PPM = 180
NOX_EURO6_LIMIT_PPM = 80
NOX_WARN_PPM = 200
NOX_CRITICAL_PPM = 500

# Lambda (stoichiometric = 1.0 for diesel, ~1.3–4.0 typical)
LAMBDA_LEAN_WARN = 4.5
LAMBDA_RICH_WARN = 1.1   # diesel should always run lean


class NOxSensor(BaseSensor):
    name = 'nox'
    unit = 'ppm'

    def read(self) -> SensorReading:
        nox_upstream_ppm = round(random.uniform(20, 550), 1)
        nox_downstream_ppm = round(random.uniform(10, 300), 1)
        lambda_value = round(random.uniform(0.9, 5.0), 3)
        scr_efficiency_pct = round(
            max(0, (1 - nox_downstream_ppm / max(nox_upstream_ppm, 1)) * 100), 1
        )

        fault_codes = []
        status = 'ok'

        if nox_downstream_ppm >= NOX_CRITICAL_PPM:
            status = 'critical'
            fault_codes.append(FaultCode('P2200', f'NOx critically high ({nox_downstream_ppm:.0f} ppm) — emissions system failure', 'critical'))
        elif nox_downstream_ppm >= NOX_WARN_PPM:
            status = 'warning'
            fault_codes.append(FaultCode('P2201', f'NOx above Euro 5 limit ({nox_downstream_ppm:.0f} ppm)', 'warning'))
        elif nox_downstream_ppm >= NOX_EURO6_LIMIT_PPM:
            fault_codes.append(FaultCode('P229F', f'NOx above Euro 6 limit ({nox_downstream_ppm:.0f} ppm)', 'info'))

        if lambda_value < LAMBDA_RICH_WARN:
            if status != 'critical':
                status = 'warning'
            fault_codes.append(FaultCode('P0130', f'Lambda rich condition ({lambda_value:.3f}) — unburnt fuel in exhaust', 'warning'))
        elif lambda_value > LAMBDA_LEAN_WARN:
            fault_codes.append(FaultCode('P0136', f'Lambda excessively lean ({lambda_value:.3f})', 'info'))

        return self._make_reading(
            value=nox_downstream_ppm,
            status=status,
            fault_codes=fault_codes,
            metadata={
                'nox_upstream_ppm': nox_upstream_ppm,
                'nox_downstream_ppm': nox_downstream_ppm,
                'lambda': lambda_value,
                'scr_efficiency_pct': scr_efficiency_pct,
                'euro5_limit_ppm': NOX_EURO5_LIMIT_PPM,
                'euro6_limit_ppm': NOX_EURO6_LIMIT_PPM,
            },
        )
