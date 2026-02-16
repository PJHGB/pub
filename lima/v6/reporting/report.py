"""
Diagnostic report data model and generator.

Collects a snapshot of all sensor readings and fault codes into a
structured DiagnosticReport that can be rendered by any reporter.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from ..sensors.base import BaseSensor, SensorReading, FaultCode


@dataclass
class DiagnosticReport:
    vehicle_id: str
    generated_at: datetime
    readings: list[SensorReading]
    duration_s: float = 0.0
    notes: str = ''

    @property
    def fault_codes(self) -> list[FaultCode]:
        codes = []
        for r in self.readings:
            codes.extend(r.fault_codes)
        return codes

    @property
    def critical_count(self) -> int:
        return sum(1 for fc in self.fault_codes if fc.severity == 'critical')

    @property
    def warning_count(self) -> int:
        return sum(1 for fc in self.fault_codes if fc.severity == 'warning')

    @property
    def overall_status(self) -> str:
        if any(r.status == 'critical' for r in self.readings):
            return 'CRITICAL'
        if any(r.status == 'warning' for r in self.readings):
            return 'WARNING'
        return 'OK'

    def to_dict(self) -> dict:
        return {
            'vehicle_id': self.vehicle_id,
            'generated_at': self.generated_at.isoformat(),
            'overall_status': self.overall_status,
            'duration_s': self.duration_s,
            'fault_summary': {
                'critical': self.critical_count,
                'warning': self.warning_count,
                'total': len(self.fault_codes),
            },
            'readings': [r.to_dict() for r in self.readings],
            'notes': self.notes,
        }


class ReportGenerator:
    def __init__(self, sensors: list[BaseSensor], vehicle_id: str = 'BMW-TDV6'):
        self._sensors = sensors
        self._vehicle_id = vehicle_id

    def generate(self, notes: str = '') -> DiagnosticReport:
        start = datetime.utcnow()
        readings = [sensor.read() for sensor in self._sensors]
        end = datetime.utcnow()
        duration = (end - start).total_seconds()

        return DiagnosticReport(
            vehicle_id=self._vehicle_id,
            generated_at=start,
            readings=readings,
            duration_s=duration,
            notes=notes,
        )
