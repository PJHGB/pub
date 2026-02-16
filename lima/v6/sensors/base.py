from dataclasses import dataclass, field
from typing import Any, Optional
from datetime import datetime


@dataclass
class FaultCode:
    code: str
    description: str
    severity: str  # 'info', 'warning', 'critical'
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'description': self.description,
            'severity': self.severity,
            'timestamp': self.timestamp.isoformat(),
        }


@dataclass
class SensorReading:
    sensor_name: str
    value: Any
    unit: str
    status: str  # 'ok', 'warning', 'critical', 'unknown'
    timestamp: datetime = field(default_factory=datetime.utcnow)
    fault_codes: list[FaultCode] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'sensor': self.sensor_name,
            'value': self.value,
            'unit': self.unit,
            'status': self.status,
            'timestamp': self.timestamp.isoformat(),
            'fault_codes': [fc.to_dict() for fc in self.fault_codes],
            'metadata': self.metadata,
        }


class BaseSensor:
    """Abstract base for all BMW TDV6 sensor modules."""

    name: str = 'base_sensor'
    unit: str = ''

    def read(self) -> SensorReading:
        raise NotImplementedError

    def _make_reading(
        self,
        value: Any,
        status: str,
        fault_codes: Optional[list[FaultCode]] = None,
        metadata: Optional[dict] = None,
    ) -> SensorReading:
        return SensorReading(
            sensor_name=self.name,
            value=value,
            unit=self.unit,
            status=status,
            fault_codes=fault_codes or [],
            metadata=metadata or {},
        )
