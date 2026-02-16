"""
Event bus — pub/sub broker for the BMW TDV6 diagnostic system.

Subscribers register a callable per EventType (or None for all events).
Events are dispatched synchronously by default; async dispatch uses a
background thread queue so the publisher never blocks.
"""
import threading
import queue
from collections import defaultdict
from typing import Callable, Optional
from .types import Event, EventType
from ..logging.logger import logger


class EventBus:
    def __init__(self, async_dispatch: bool = True):
        self._subscribers: dict[Optional[EventType], list[Callable[[Event], None]]] = defaultdict(list)
        self._async = async_dispatch
        self._queue: queue.Queue[Optional[Event]] = queue.Queue()
        self._dispatch_thread: Optional[threading.Thread] = None
        self._running = False

        if async_dispatch:
            self._start_dispatch_thread()

    def _start_dispatch_thread(self):
        self._running = True
        self._dispatch_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatch_thread.start()

    def _dispatch_loop(self):
        while self._running:
            try:
                event = self._queue.get(timeout=0.1)
                if event is None:
                    break
                self._dispatch_sync(event)
                self._queue.task_done()
            except queue.Empty:
                continue

    def _dispatch_sync(self, event: Event):
        handlers = (
            self._subscribers.get(event.event_type, [])
            + self._subscribers.get(None, [])
        )
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                logger.error(f'EventBus handler error [{handler.__name__}]: {exc}')

    def publish(self, event: Event):
        if self._async:
            self._queue.put(event)
        else:
            self._dispatch_sync(event)

    def subscribe(self, handler: Callable[[Event], None], event_type: Optional[EventType] = None):
        """Subscribe to a specific EventType, or None to receive all events."""
        self._subscribers[event_type].append(handler)

    def unsubscribe(self, handler: Callable[[Event], None], event_type: Optional[EventType] = None):
        try:
            self._subscribers[event_type].remove(handler)
        except ValueError:
            pass

    def stop(self):
        self._running = False
        if self._async:
            self._queue.put(None)
            if self._dispatch_thread:
                self._dispatch_thread.join(timeout=2)
