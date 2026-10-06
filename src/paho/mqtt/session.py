"""Pluggable session persistence for in-flight QoS > 0 messages.

This module defines the interface between `paho.mqtt.client.Client` and a
user-provided session store. The client only relies on the four methods
defined by `SessionStore`; any object implementing them (duck typing) can
be passed as the ``session_store`` argument of `Client`.
"""

from __future__ import annotations

import dataclasses

#: State of an outgoing message for which the PUBLISH packet has been sent
#: and the client is waiting for PUBACK (QoS 1) or PUBREC (QoS 2).
SESSION_STATE_PUBLISH = "publish"

#: State of an outgoing QoS 2 message for which PUBREC has been received and
#: the client is waiting for PUBCOMP. On session restore such a message must
#: be completed by resending PUBREL, not by republishing.
SESSION_STATE_PUBREL = "pubrel"


@dataclasses.dataclass
class SessionMessage:
    """Record describing a single in-flight outgoing QoS > 0 message.

    This is the unit of data exchanged between the client and a
    `SessionStore`. Stores are free to serialize it any way they like
    (e.g. a sqlite row, a JSON document) as long as `load` returns
    equivalent records.
    """

    client_id: str
    """The client id that owns this record (str)."""

    mid: int
    """The message id (int)."""

    topic: bytes
    """The topic the message was published on (bytes, UTF-8 encoded)."""

    payload: bytes
    """The message payload (bytes)."""

    qos: int
    """The Quality of Service level (1 or 2)."""

    retain: bool = False
    """The retain flag of the original publish."""

    dup: bool = False
    """Whether the PUBLISH was already a retransmission."""

    state: str = SESSION_STATE_PUBLISH
    """One of `SESSION_STATE_PUBLISH` or `SESSION_STATE_PUBREL`."""


class SessionStore:
    """Interface for a pluggable session store.

    Implementations must provide the four methods below. The client calls
    them from its network thread, so implementations should be thread-safe
    with respect to the application's own use of the client.

    Any exception raised by a method is caught by the client, logged, and
    (for `save`/`remove`) reported through the usual ``on_disconnect``
    path so the network loop is not taken down.
    """

    def load(self, client_id: str) -> list[SessionMessage]:
        """Return all persisted records for ``client_id``.

        Must return an iterable of `SessionMessage`; an empty list when
        nothing is stored.
        """
        raise NotImplementedError

    def save(self, record: SessionMessage) -> None:
        """Persist ``record``, replacing any existing record with the same
        ``(client_id, mid)``."""
        raise NotImplementedError

    def remove(self, mid: int) -> None:
        """Delete the record for ``mid``. Must not fail if no such record
        exists."""
        raise NotImplementedError

    def clear(self, client_id: str) -> None:
        """Delete all records for ``client_id``."""
        raise NotImplementedError
