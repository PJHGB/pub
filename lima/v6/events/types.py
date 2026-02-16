from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class EventType(Enum):
    SENSOR_READING = 'sensor_reading'
    FAULT_CODE_RAISED = 'fault_code_raised'
    FAULT_CODE_CLEARED = 'fault_code_cleared'
    THRESHOLD_BREACH = 'threshold_breach'
    SYSTEM_STATUS = 'system_status'
    ENGINE_START = 'engine_start'
    ENGINE_STOP = 'engine_stop'
    REGEN_START = 'regen_start'
    REGEN_COMPLETE = 'regen_complete'
    CONNECTION_ESTABLISHED = 'connection_established'
    CONNECTION_LOST = 'connection_lost'


class Severity(Enum):
    INFO = 'info'
    WARNING = 'warning'
    CRITICAL = 'critical'


@dataclass
class Event:
    event_type: EventType
    source: str
    severity: Severity
    data: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self):
        if self.event_id is None:
            import uuid
            self.event_id = str(uuid.uuid4())

    def to_dict(self) -> dict:
        return {
            'event_id': self.event_id,
            'event_type': self.event_type.value,
            'source': self.source,
            'severity': self.severity.value,
            'timestamp': self.timestamp.isoformat(),
            'data': self.data if not hasattr(self.data, 'to_dict') else self.data.to_dict(),
            'correlation_id': self.correlation_id,
        }
