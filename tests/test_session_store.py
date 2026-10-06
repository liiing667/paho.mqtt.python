"""Tests for the pluggable session store (persisted in-flight publishes)."""

import threading
import time

import pytest

import paho.mqtt.client as client
import tests.paho_test as paho_test
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.session import (
    SESSION_STATE_PUBLISH,
    SESSION_STATE_PUBREL,
    SessionMessage,
    SessionStore,
)

from tests.testsupport.broker import FakeBroker

CLIENT_ID = "session-store-test"


class DictSessionStore(SessionStore):
    """In-memory SessionStore standing in for a sqlite implementation."""

    def __init__(self):
        self._records = {}
        self.cleared = []
        self.lock = threading.Lock()

    def load(self, client_id):
        with self.lock:
            return [
                dataclasses_replace(r)
                for (cid, _mid), r in self._records.items()
                if cid == client_id
            ]

    def save(self, record):
        with self.lock:
            self._records[(record.client_id, record.mid)] = record

    def remove(self, mid):
        with self.lock:
            for key in [k for k in self._records if k[1] == mid]:
                del self._records[key]

    def clear(self, client_id):
        with self.lock:
            self.cleared.append(client_id)
            for key in [k for k in self._records if k[0] == client_id]:
                del self._records[key]

    def record(self, mid):
        return self._records.get((CLIENT_ID, mid))

    def is_empty(self):
        return not self._records


def dataclasses_replace(record):
    # Return a copy so callers mutating the restored message cannot corrupt
    # the store, the way a persistent implementation would naturally do.
    return SessionMessage(**record.__dict__)


class FailingStore(DictSessionStore):
    def __init__(self, failing_method):
        super().__init__()
        self.failing_method = failing_method
        self.errors = []

    def _fail(self, name):
        if name == self.failing_method:
            err = RuntimeError(f"store {name} boom")
            self.errors.append(err)
            raise err

    def load(self, client_id):
        self._fail("load")
        return super().load(client_id)

    def save(self, record):
        self._fail("save")
        super().save(record)

    def remove(self, mid):
        self._fail("remove")
        super().remove(mid)

    def clear(self, client_id):
        self._fail("clear")
        super().clear(client_id)


def wait_for(predicate, timeout=5.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def make_client(broker, store, *, client_id=CLIENT_ID, clean_session=False):
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        client_id,
        clean_session=clean_session,
        protocol=client.MQTTv311,
        transport=broker.transport,
        session_store=store,
    )
    return mqttc


def handshake(broker, mqttc, *, clean_session=False, client_id=CLIENT_ID):
    mqttc.connect_async("localhost", broker.port)
    mqttc.loop_start()
    broker.start()

    connect_packet = paho_test.gen_connect(
        client_id, keepalive=60, clean_session=clean_session)
    assert broker.receive_packet(1000) == connect_packet

    connack_packet = paho_test.gen_connack(rc=0)
    assert broker.send_packet(connack_packet) == len(connack_packet)


@pytest.fixture
def broker():
    b = FakeBroker("tcp")
    yield b
    b.finish()


class TestSessionStore:
    def test_invalid_store_rejected(self):
        class NotAStore:
            pass

        with pytest.raises(ValueError, match="load"):
            client.Client(
                CallbackAPIVersion.VERSION2,
                CLIENT_ID,
                clean_session=False,
                protocol=client.MQTTv311,
                session_store=NotAStore(),
            )

    def test_qos1_saved_after_send_and_removed_on_puback(self, broker):
        store = DictSessionStore()
        mqttc = make_client(broker, store)
        published = threading.Event()
        mqttc.on_publish = lambda *args: published.set()
        handshake(broker, mqttc)

        info = mqttc.publish("session/qos1", "payload-1", qos=1)
        expected = paho_test.gen_publish(
            "session/qos1", qos=1, mid=info.mid, payload="payload-1")
        assert broker.receive_packet(1000) == expected

        record = store.record(info.mid)
        assert record is not None
        assert record.mid == info.mid
        assert record.topic == b"session/qos1"
        assert record.payload == b"payload-1"
        assert record.qos == 1
        assert record.retain is False
        assert record.state == SESSION_STATE_PUBLISH
        assert store.is_empty() is False

        broker.send_packet(paho_test.gen_puback(info.mid))
        assert published.wait(5.0)
        assert wait_for(store.is_empty)

        mqttc.disconnect()
        mqttc.loop_stop()

    def test_qos1_retain_persisted(self, broker):
        store = DictSessionStore()
        mqttc = make_client(broker, store)
        handshake(broker, mqttc)

        info = mqttc.publish("session/retain", "r", qos=1, retain=True)
        expected = paho_test.gen_publish(
            "session/retain", qos=1, mid=info.mid, payload="r", retain=True)
        assert broker.receive_packet(1000) == expected

        record = store.record(info.mid)
        assert record.retain is True

        broker.send_packet(paho_test.gen_puback(info.mid))
        mqttc.disconnect()
        mqttc.loop_stop()

    def test_qos0_not_persisted(self, broker):
        store = DictSessionStore()
        mqttc = make_client(broker, store)
        handshake(broker, mqttc)

        mqttc.publish("session/qos0", "payload-0", qos=0)
        expected = paho_test.gen_publish(
            "session/qos0", qos=0, payload="payload-0")
        assert broker.receive_packet(1000) == expected
        assert store.is_empty()

        mqttc.disconnect()
        mqttc.loop_stop()

    def test_qos2_pubrel_phase_saved_and_removed_on_pubcomp(self, broker):
        store = DictSessionStore()
        mqttc = make_client(broker, store)
        published = threading.Event()
        mqttc.on_publish = lambda *args: published.set()
        handshake(broker, mqttc)

        info = mqttc.publish("session/qos2", "payload-2", qos=2)
        expected_publish = paho_test.gen_publish(
            "session/qos2", qos=2, mid=info.mid, payload="payload-2")
        assert broker.receive_packet(1000) == expected_publish
        assert store.record(info.mid).state == SESSION_STATE_PUBLISH

        broker.send_packet(paho_test.gen_pubrec(info.mid))
        assert broker.receive_packet(1000) == paho_test.gen_pubrel(info.mid)
        assert wait_for(
            lambda: store.record(info.mid) is not None
            and store.record(info.mid).state == SESSION_STATE_PUBREL)

        broker.send_packet(paho_test.gen_pubcomp(info.mid))
        assert published.wait(5.0)
        assert wait_for(store.is_empty)

        mqttc.disconnect()
        mqttc.loop_stop()

    def test_restart_resends_publish_with_original_mid(self, broker):
        store = DictSessionStore()

        mqttc = make_client(broker, store)
        published = threading.Event()
        mqttc.on_publish = lambda *args: published.set()
        handshake(broker, mqttc)

        info = mqttc.publish("session/restart", "payload-x", qos=1)
        assert broker.receive_packet(1000) == paho_test.gen_publish(
            "session/restart", qos=1, mid=info.mid, payload="payload-x")
        assert info.mid == 1
        # "Process restart": drop the client while the publish is unacked.
        mqttc.disconnect()
        mqttc.loop_stop()
        broker.close_connection()

        # A brand new client object (same client id and store) resumes.
        mqttc2 = make_client(broker, store)
        mqttc2.on_publish = lambda *args: published.set()
        handshake(broker, mqttc2)

        # Resent with the original mid and the DUP flag set.
        assert broker.receive_packet(1000) == paho_test.gen_publish(
            "session/restart", qos=1, mid=1, payload="payload-x", dup=True)

        # The restored mid must not be reused by a subsequent publish.
        new_info = mqttc2.publish("session/next", "payload-y", qos=1)
        assert new_info.mid == 2

        broker.send_packet(paho_test.gen_puback(1))
        broker.send_packet(paho_test.gen_puback(2))
        assert wait_for(store.is_empty)

        mqttc2.disconnect()
        mqttc2.loop_stop()

    def test_restart_pubrel_phase_resends_pubrel_only(self, broker):
        store = DictSessionStore()

        mqttc = make_client(broker, store)
        handshake(broker, mqttc)

        info = mqttc.publish("session/pubrel", "payload-2", qos=2)
        assert broker.receive_packet(1000) == paho_test.gen_publish(
            "session/pubrel", qos=2, mid=info.mid, payload="payload-2")
        broker.send_packet(paho_test.gen_pubrec(info.mid))
        assert broker.receive_packet(1000) == paho_test.gen_pubrel(info.mid)

        # Restart while waiting for PUBCOMP.
        mqttc.disconnect()
        mqttc.loop_stop()
        broker.close_connection()

        published = threading.Event()
        mqttc2 = make_client(broker, store)
        mqttc2.on_publish = lambda *args: published.set()
        handshake(broker, mqttc2)

        # First outgoing packet must be the PUBREL, not a duplicate PUBLISH.
        assert broker.receive_packet(1000) == paho_test.gen_pubrel(info.mid)
        assert store.record(info.mid).state == SESSION_STATE_PUBREL

        broker.send_packet(paho_test.gen_pubcomp(info.mid))
        assert published.wait(5.0)
        assert wait_for(store.is_empty)

        mqttc2.disconnect()
        mqttc2.loop_stop()

    def test_clean_session_start_clears_store(self, broker):
        store = DictSessionStore()
        store.save(SessionMessage(
            client_id=CLIENT_ID,
            mid=42,
            topic=b"session/old",
            payload=b"stale",
            qos=1,
        ))

        mqttc = make_client(broker, store, clean_session=True)
        handshake(broker, mqttc, clean_session=True)

        assert store.cleared == [CLIENT_ID]
        assert store.is_empty()

        mqttc.disconnect()
        # The next packet is the DISCONNECT: no stale PUBLISH was resent.
        assert broker.receive_packet(1000) == paho_test.gen_disconnect()
        mqttc.loop_stop()

    def test_load_returns_restored_session_without_prior_client(self, broker):
        store = DictSessionStore()
        store.save(SessionMessage(
            client_id=CLIENT_ID,
            mid=7,
            topic=b"session/seed",
            payload=b"seed",
            qos=1,
        ))

        mqttc = make_client(broker, store)
        handshake(broker, mqttc)

        assert broker.receive_packet(1000) == paho_test.gen_publish(
            "session/seed", qos=1, mid=7, payload="seed", dup=True)

        mqttc.disconnect()
        mqttc.loop_stop()

    def test_save_failure_logs_and_disconnects_without_killing_thread(self, broker):
        store = FailingStore("save")
        mqttc = make_client(broker, store)
        disconnected = threading.Event()
        logs = []

        def on_disconnect(*args):
            disconnected.set()

        def on_log(mqttc, userdata, level, buf):
            logs.append(buf)

        mqttc.on_disconnect = on_disconnect
        mqttc.on_log = on_log
        handshake(broker, mqttc)

        mqttc.publish("session/boom", "payload", qos=1)
        assert disconnected.wait(5.0)
        assert any("Session store save failed" in line for line in logs)

        # The network loop thread is still alive and can be stopped cleanly.
        mqttc.loop_stop()

    def test_remove_failure_logs_and_disconnects(self, broker):
        store = FailingStore("remove")
        mqttc = make_client(broker, store)
        disconnected = threading.Event()
        mqttc.on_disconnect = lambda *args: disconnected.set()
        handshake(broker, mqttc)

        info = mqttc.publish("session/boom", "payload", qos=1)
        assert broker.receive_packet(1000)
        broker.send_packet(paho_test.gen_puback(info.mid))
        assert disconnected.wait(5.0)
        mqttc.loop_stop()

    def test_load_failure_does_not_prevent_connect(self, broker):
        store = FailingStore("load")
        mqttc = make_client(broker, store)
        connected = threading.Event()
        mqttc.on_connect = lambda *args: connected.set()
        handshake(broker, mqttc)
        assert connected.wait(5.0)

        mqttc.disconnect()
        mqttc.loop_stop()
