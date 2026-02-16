"""
In-process pub/sub event bus.

Topics follow a dot-separated hierarchy:
    odds.betfair
    odds.matchbook
    feed.started.<exchange>
    feed.stopped.<exchange>
    feed.error.<exchange>

Wildcard subscriptions are supported:
    odds.*          matches odds.betfair, odds.matchbook, ...
    feed.error.*    matches any exchange error

Usage
-----
    from pubsub import EventBus, Event

    bus = EventBus()

    # Subscribe
    def on_odds(event: Event):
        print(event.exchange, len(event.data))

    sub = bus.subscribe("odds.*", on_odds)

    # Publish (from any thread)
    bus.publish("odds.betfair", exchange="betfair", data=markets)

    # Unsubscribe
    bus.unsubscribe(sub)
"""

import fnmatch
import logging
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

log = logging.getLogger(__name__)


@dataclass
class Event:
    topic: str
    exchange: str
    data: Any                          # payload — list[Market] for odds topics
    timestamp: float = field(default_factory=time.time)


# Subscriber callable type
Handler = Callable[[Event], None]


@dataclass
class Subscription:
    id: str
    pattern: str
    handler: Handler

    def matches(self, topic: str) -> bool:
        return fnmatch.fnmatch(topic, self.pattern)


class EventBus:
    """
    Thread-safe pub/sub bus.  Handlers are called synchronously in the
    publisher's thread by default, or dispatched to a background
    delivery thread when async_dispatch=True.
    """

    def __init__(self, async_dispatch: bool = False):
        self._subs: list[Subscription] = []
        self._lock = threading.Lock()
        self._async = async_dispatch

        if async_dispatch:
            # Unbounded queue fed by publishers, drained by one delivery thread
            import queue
            self._queue: queue.Queue = queue.Queue()
            self._delivery_thread = threading.Thread(
                target=self._delivery_loop,
                name="pubsub-delivery",
                daemon=True,
            )
            self._delivery_thread.start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe(self, pattern: str, handler: Handler) -> Subscription:
        """
        Register *handler* to be called whenever an event is published
        on a topic matching *pattern*.  Supports fnmatch wildcards (* ?).

        Returns the Subscription object (pass to unsubscribe to remove).
        """
        sub = Subscription(id=str(uuid4()), pattern=pattern, handler=handler)
        with self._lock:
            self._subs.append(sub)
        log.debug("[bus] subscribed %s → %r", sub.id[:8], pattern)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        with self._lock:
            self._subs = [s for s in self._subs if s.id != sub.id]
        log.debug("[bus] unsubscribed %s", sub.id[:8])

    def publish(self, topic: str, exchange: str, data: Any = None) -> None:
        """
        Publish an event on *topic*.  Matched handlers are invoked
        in-thread (sync) or queued for async delivery.
        """
        event = Event(topic=topic, exchange=exchange, data=data)
        with self._lock:
            matched = [s for s in self._subs if s.matches(topic)]

        if self._async:
            for sub in matched:
                self._queue.put((sub, event))
        else:
            for sub in matched:
                self._invoke(sub, event)

    def publish_event(self, event: Event) -> None:
        """Publish a pre-built Event object."""
        self.publish(event.topic, event.exchange, event.data)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _invoke(self, sub: Subscription, event: Event) -> None:
        try:
            sub.handler(event)
        except Exception:
            log.error(
                "[bus] handler error for topic %r (sub %s):\n%s",
                event.topic, sub.id[:8], traceback.format_exc(),
            )

    def _delivery_loop(self) -> None:
        import queue
        while True:
            try:
                sub, event = self._queue.get(timeout=1)
                self._invoke(sub, event)
            except queue.Empty:
                continue
            except Exception:
                log.error("[bus] delivery loop error:\n%s", traceback.format_exc())


# Module-level default bus — importable anywhere without passing instances
default_bus: EventBus = EventBus(async_dispatch=True)
