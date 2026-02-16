"""
Continuous event feed — polls all registered sensors on an interval
and publishes SensorReading events (and any fault code events) to the bus.
"""
import threading
import time
from typing import Optional
from .bus import EventBus
from .types import Event, EventType, Severity
from ..sensors.base import BaseSensor, SensorReading
from ..logging.logger import logger

# Map sensor status strings to Severity
_STATUS_SEVERITY = {
    'ok': Severity.INFO,
    'warning': Severity.WARNING,
    'critical': Severity.CRITICAL,
    'unknown': Severity.INFO,
}


class EventFeed:
    """Polls all sensor modules at `interval_ms` and publishes events to the bus."""

    def __init__(self, bus: EventBus, sensors: list[BaseSensor], interval_ms: int = 500):
        self._bus = bus
        self._sensors = sensors
        self._interval = interval_ms / 1000.0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._tick_count = 0

    def _poll_once(self):
        self._tick_count += 1
        for sensor in self._sensors:
            try:
                reading: SensorReading = sensor.read()
                severity = _STATUS_SEVERITY.get(reading.status, Severity.INFO)

                # Always publish the sensor reading event
                self._bus.publish(Event(
                    event_type=EventType.SENSOR_READING,
                    source=sensor.name,
                    severity=severity,
                    data=reading.to_dict(),
                    correlation_id=str(self._tick_count),
                ))

                # Publish individual fault code events
                for fc in reading.fault_codes:
                    self._bus.publish(Event(
                        event_type=EventType.FAULT_CODE_RAISED,
                        source=sensor.name,
                        severity=Severity(fc.severity) if fc.severity in ('info', 'warning', 'critical') else Severity.INFO,
                        data=fc.to_dict(),
                        correlation_id=str(self._tick_count),
                    ))

                # Publish threshold breach if status is not ok
                if reading.status in ('warning', 'critical'):
                    self._bus.publish(Event(
                        event_type=EventType.THRESHOLD_BREACH,
                        source=sensor.name,
                        severity=severity,
                        data={
                            'sensor': reading.sensor_name,
                            'value': reading.value,
                            'unit': reading.unit,
                            'status': reading.status,
                        },
                        correlation_id=str(self._tick_count),
                    ))

            except Exception as exc:
                logger.error(f'EventFeed poll error [{sensor.name}]: {exc}')

    def _run(self):
        logger.info('EventFeed started')
        while self._running:
            self._poll_once()
            time.sleep(self._interval)
        logger.info('EventFeed stopped')

    def start(self):
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    @property
    def tick_count(self) -> int:
        return self._tick_count
