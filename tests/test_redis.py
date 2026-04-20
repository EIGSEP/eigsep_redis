"""Unit tests for the eigsep_redis bus primitives.

Pure-package tests covering the Transport and writer/reader surfaces.
End-to-end integration tests that exercise corr / VNA alongside
metadata live in ``eigsep_observing/tests/test_redis.py`` — which
imports both packages — and must stay green when this package is
upgraded.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from eigsep_redis import (
    ConfigStore,
    HeartbeatReader,
    HeartbeatWriter,
    MetadataSnapshotReader,
    MetadataStreamReader,
    MetadataWriter,
    StatusReader,
    StatusWriter,
)
from eigsep_redis.keys import METADATA_HASH
from eigsep_redis.testing import DummyEigsepRedis, DummyTransport


class _BusBundle:
    """All-surfaces test bus — convenience wrapper used only by the
    bus-primitive tests below.

    Production code builds only the per-role surfaces it needs
    (``EigObserver`` / ``PandaClient`` / ``EigsepFpga`` each pick their
    own subset, in the ``eigsep_observing`` repo). This helper exists
    so producer↔consumer round-trip tests stay readable without
    re-introducing a composition class into ``src/``.
    """

    def __init__(self, transport):
        self.transport = transport
        self.metadata = MetadataWriter(transport)
        self.metadata_snapshot = MetadataSnapshotReader(transport)
        self.metadata_stream = MetadataStreamReader(transport)
        self.status = StatusWriter(transport)
        self.status_reader = StatusReader(transport)
        self.heartbeat = HeartbeatWriter(transport)
        self.heartbeat_reader = HeartbeatReader(transport)
        self.config = ConfigStore(transport)

    @property
    def r(self):
        return self.transport.r

    def reset(self):
        return self.transport.reset()

    def _set_last_read_id(self, stream, read_id):
        return self.transport._set_last_read_id(stream, read_id)

    def add_raw(self, key, value, ex=None):
        return self.transport.add_raw(key, value, ex=ex)

    def get_raw(self, key):
        return self.transport.get_raw(key)

    @property
    def data_streams(self):
        return self.transport._streams_from_set("data_streams")


@pytest.fixture
def server():
    return _BusBundle(DummyTransport())


@pytest.fixture
def client(server):
    t = DummyTransport()
    # share the underlying fakeredis so both clients talk to the
    # same in-memory DB but keep independent last-read-id state
    t.r = server.transport.r
    return _BusBundle(t)


def test_metadata(server, client):
    assert server.data_streams == {}  # initially empty

    for acc_cnt in range(10):
        before = time.time()
        client.metadata.add("acc_cnt", acc_cnt)
        after = time.time()
        assert client.r.smembers("data_streams") == {b"stream:acc_cnt"}
        assert server.r.smembers("data_streams") == {b"stream:acc_cnt"}
        if acc_cnt == 0:
            assert "stream:acc_cnt" in server.data_streams
        assert server.metadata_snapshot.get(keys="acc_cnt") == acc_cnt
        assert server.metadata_snapshot.get(keys=["acc_cnt"]) == {
            "acc_cnt": acc_cnt
        }
        live = server.metadata_snapshot.get()
        assert "acc_cnt_ts" in live
        ts = live.pop("acc_cnt_ts")
        assert isinstance(ts, float)
        assert before <= ts <= after
        assert live == {"acc_cnt": acc_cnt}

    # With fresh reader cache, drain of just-added data returns nothing
    # because the stream position starts at $ (new messages only).
    metadata = server.metadata_stream.drain(stream_keys="acc_cnt")
    assert metadata == {}

    # Multiple streams registered independently.
    test_date = "2025-06-02T16:25:15.089640"
    client.metadata.add("update_date", test_date)
    live = server.metadata_snapshot.get()
    assert "acc_cnt_ts" in live
    assert "update_date_ts" in live
    assert "update_date" in live
    assert set(server.data_streams.keys()) == {
        "stream:acc_cnt",
        "stream:update_date",
    }

    with pytest.raises(TypeError):
        server.metadata_snapshot.get(keys=[1])

    server.reset()
    assert server.data_streams == {}


def _backdate_ts(server, key, seconds_ago):
    """Rewrite METADATA_HASH's ``{key}_ts`` to simulate sensor
    silence. Paired with MetadataWriter.add, which stamps the current
    Unix time; this replaces it with a past value so the freshness
    check fires deterministically."""
    past = time.time() - seconds_ago
    server.r.hset(
        METADATA_HASH,
        f"{key}_ts",
        json.dumps(past).encode("utf-8"),
    )


def test_metadata_snapshot_fresh_no_warning(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_snapshot.get()
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_stale_warns_but_returns_value(
    server, client, caplog
):
    client.metadata.add("acc_cnt", 7)
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        val = server.metadata_snapshot.get("acc_cnt")
    assert val == 7
    stale = [r for r in caplog.records if "is stale" in r.message]
    assert len(stale) == 1
    assert "acc_cnt" in stale[0].message


def test_metadata_snapshot_stale_warns_on_full_get(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    client.metadata.add("temp", 25.5)
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        m = server.metadata_snapshot.get()
    assert m["acc_cnt"] == 1 and m["temp"] == 25.5
    messages = [r.message for r in caplog.records if "is stale" in r.message]
    assert any("acc_cnt" in msg for msg in messages)
    assert not any("temp" in msg for msg in messages)


def test_metadata_snapshot_missing_ts_silent(server, caplog):
    """Pre-timestamp entries (or direct hset bypasses) must not
    trigger false positives — freshness is simply unknown."""
    server.r.hset(METADATA_HASH, "legacy", json.dumps(42).encode("utf-8"))
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        val = server.metadata_snapshot.get("legacy")
    assert val == 42
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_malformed_ts_silent(server, client, caplog):
    """MetadataWriter.add always writes a valid Unix-seconds ``_ts``,
    so the non-numeric branch is unreachable via the writer. Overwrite
    ``_ts`` directly via hset to simulate a non-compliant producer or
    manual redis intervention — the only way to exercise this boundary.

    Non-numeric ``_ts`` (e.g. a leftover ISO string from a pre-Unix-time
    writer) is treated as freshness-unknown — skipped, not warned."""
    client.metadata.add("acc_cnt", 1)
    server.r.hset(
        METADATA_HASH,
        "acc_cnt_ts",
        json.dumps("not-a-timestamp").encode("utf-8"),
    )
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_snapshot.get()
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_staleness_can_be_disabled(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    _backdate_ts(server, "acc_cnt", seconds_ago=3600)
    try:
        server.metadata_snapshot.max_age_s = float("inf")
        with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
            server.metadata_snapshot.get()
    finally:
        server.metadata_snapshot.max_age_s = MetadataSnapshotReader.max_age_s
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_snapshot_staleness_restricted_to_requested_keys(
    server, client, caplog
):
    client.metadata.add("acc_cnt", 1)
    client.metadata.add("temp", 25.5)
    _backdate_ts(server, "temp", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_snapshot.get("acc_cnt")
    assert not any("is stale" in r.message for r in caplog.records)


def test_metadata_stream_silent_fresh_no_warning(server, client, caplog):
    """A stream that returned no entries this drain but whose
    panda-side ``_ts`` is recent is just slow — no warning."""
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()  # establish position past the seed
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        out = server.metadata_stream.drain()
    assert out == {}
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_metadata_stream_silent_stale_warns(server, client, caplog):
    """A stream that returned no entries this drain AND whose
    ``_ts`` is older than ``max_age_s`` warns once."""
    client.metadata.add("acc_cnt", 7)
    server.metadata_stream.drain()
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        out = server.metadata_stream.drain()
    assert out == {}
    stale = [
        r for r in caplog.records if "drained empty and is stale" in r.message
    ]
    assert len(stale) == 1
    assert "stream:acc_cnt" in stale[0].message


def test_metadata_stream_with_entries_skips_check(server, client, caplog):
    """A stream that returned entries this drain is fresh by
    definition; even an old ``_ts`` (impossible in practice — the
    writer restamps ``_ts`` on every add — but cheap to assert)
    must not warn."""
    client.metadata.add("acc_cnt", 1)
    server._set_last_read_id("stream:acc_cnt", "0-0")
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        out = server.metadata_stream.drain()
    assert out["stream:acc_cnt"] == [1]
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_metadata_stream_stale_warning_throttled(server, client, caplog):
    """At the corr cadence (~4 Hz) a permanently dead sensor would
    spam the log; the per-stream throttle suppresses repeats inside
    ``warn_interval_s``."""
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()
    _backdate_ts(server, "acc_cnt", seconds_ago=120)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        for _ in range(5):
            server.metadata_stream.drain()
    stale = [
        r for r in caplog.records if "drained empty and is stale" in r.message
    ]
    assert len(stale) == 1


def test_metadata_stream_stale_missing_ts_silent(server, client, caplog):
    """Direct ``hset`` bypasses or pre-timestamp entries leave
    ``_ts`` absent; freshness is unknown, so stay silent."""
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()
    server.r.hdel(METADATA_HASH, "acc_cnt_ts")
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
        server.metadata_stream.drain()
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_metadata_stream_stale_can_be_disabled(server, client, caplog):
    client.metadata.add("acc_cnt", 1)
    server.metadata_stream.drain()
    _backdate_ts(server, "acc_cnt", seconds_ago=3600)
    try:
        server.metadata_stream.max_age_s = float("inf")
        with caplog.at_level(logging.WARNING, logger="eigsep_redis.metadata"):
            server.metadata_stream.drain()
    finally:
        server.metadata_stream.max_age_s = MetadataStreamReader.max_age_s
    assert not any(
        "drained empty and is stale" in r.message for r in caplog.records
    )


def test_raw(server):
    """Raw K/V round-trip via Transport.add_raw / get_raw."""
    payload = b"\x01\x02\x03\x04"
    server.add_raw("data:foo", payload)
    assert server.get_raw("data:foo") == payload


def test_metadata_writer_has_no_cross_bus_methods():
    """Structural guard: the metadata writer surface must not expose any
    method or attribute that could be used to write a corr or VNA payload.

    This is the whole point of the writer/reader split — wrong-stream
    writes should be unrepresentable at the type level, not runtime-
    checked.
    """
    public = {
        name for name in vars(MetadataWriter) if not name.startswith("_")
    }
    assert public == {"add", "maxlen"}, (
        f"MetadataWriter surface has grown: {public}"
    )
    for forbidden in (
        "add_corr_data",
        "add_vna_data",
        "send_status",
        "upload_config",
        "upload_corr_config",
        "upload_corr_header",
    ):
        assert not hasattr(MetadataWriter, forbidden), (
            f"MetadataWriter should not expose {forbidden!r}"
        )


def test_metadata_readers_have_no_cross_bus_methods():
    """Structural guard: metadata readers only read metadata, nothing else."""
    for cls, expected in (
        (MetadataSnapshotReader, {"get", "max_age_s"}),
        (
            MetadataStreamReader,
            {"drain", "streams", "max_age_s", "warn_interval_s"},
        ),
    ):
        public = {name for name in vars(cls) if not name.startswith("_")}
        assert public == expected, (
            f"{cls.__name__} surface has grown: {public}"
        )
        for forbidden in (
            "read_corr_data",
            "read_vna_data",
            "read_status",
            "get_corr_config",
            "get_corr_header",
        ):
            assert not hasattr(cls, forbidden), (
                f"{cls.__name__} should not expose {forbidden!r}"
            )


def test_metadata_writer_rejects_non_json_payload(server):
    """Contract: the writer is the JSON-serialization boundary."""

    class Unserializable:
        pass

    with pytest.raises(ValueError):
        server.metadata.add("broken", Unserializable())


def test_metadata_writer_rejects_bad_keys(server):
    """Contract: keys are strings, non-empty, no ':' (Redis separator)."""
    with pytest.raises(TypeError):
        server.metadata.add(123, "value")
    with pytest.raises(ValueError):
        server.metadata.add("", "value")
    with pytest.raises(ValueError):
        server.metadata.add("   ", "value")
    with pytest.raises(ValueError):
        server.metadata.add("a:b", "value")


def test_eigsep_redis_bus_classes_have_no_cross_bus_methods():
    """Structural guard for every in-package writer/reader. Each class
    should expose only the surface for its own bus. The corr/vna
    counterpart guard lives in eigsep_observing/tests/test_redis.py
    where those classes are defined.
    """
    cross_bus_methods = (
        "add_metadata",
        "get_live_metadata",
        "get_metadata",
        "add_corr_data",
        "read_corr_data",
        "add_vna_data",
        "read_vna_data",
        "upload_corr_config",
        "get_corr_config",
        "upload_corr_header",
        "get_corr_header",
        "send_status",
        "read_status",
        "upload_config",
        "get_config",
        "client_heartbeat_set",
        "client_heartbeat_check",
    )
    surfaces = {
        StatusWriter: {"send", "maxlen"},
        StatusReader: {"read", "stream"},
        HeartbeatWriter: {"set"},
        HeartbeatReader: {"check"},
        ConfigStore: {"upload", "get"},
    }
    for cls, expected in surfaces.items():
        public = {name for name in vars(cls) if not name.startswith("_")}
        assert public == expected, (
            f"{cls.__name__} surface has grown: {public}"
        )
        for forbidden in cross_bus_methods:
            if forbidden in expected:
                continue
            assert not hasattr(cls, forbidden), (
                f"{cls.__name__} should not expose {forbidden!r}"
            )


def test_add_metadata_shim_emits_deprecation_warning():
    """The picohost shim must be loud — it should disappear once
    picohost migrates to MetadataWriter.add.

    Constructs ``DummyEigsepRedis`` directly because the shim lives
    on that class, not on the per-bus writer surfaces.
    """
    shim = DummyEigsepRedis()
    with pytest.warns(DeprecationWarning, match="redis.metadata.add"):
        shim.add_metadata("via_shim", 42)
    snapshot = MetadataSnapshotReader(shim.transport)
    assert snapshot.get("via_shim") == 42


def test_metadata_drain_skips_producer_backlog(server, client):
    """Tier-2 guard for metadata streams: a consumer whose cache is
    empty must see a producer-first backlog as "past" and return an
    empty drain, not replay the backlog.
    """
    for i in range(5):
        client.metadata.add("acc_cnt", i)
    assert server.metadata_stream.drain() == {}


def test_is_alive(server, client):
    assert server.heartbeat_reader.check() is False
    client.heartbeat.set(ex=1, alive=True)
    assert server.heartbeat_reader.check() is True
    time.sleep(1.1)
    assert server.heartbeat_reader.check() is False
    client.heartbeat.set(ex=100, alive=True)
    assert server.heartbeat_reader.check() is True
    client.heartbeat.set(ex=100, alive=False)
    assert server.heartbeat_reader.check() is False
    client.heartbeat.set(ex=100, alive=True)
    assert server.heartbeat_reader.check() is True
    server.reset()
    assert server.heartbeat_reader.check() is False


def test_status(server, client):
    assert client.status_reader.stream == {"stream:status": "$"}

    with ThreadPoolExecutor(max_workers=2) as executor:
        read_future = executor.submit(server.status_reader.read)
        time.sleep(0.1)
        msg = "test"
        client.status.send(msg)
        level, status = read_future.result(timeout=2.0)
        assert status == msg
        assert level == 20  # logging.INFO

    messages = [f"status {i}" for i in range(5)]
    for msg in messages:
        client.status.send(msg)
    for expected_msg in messages:
        level, status = server.status_reader.read()
        assert status == expected_msg
        assert level == 20

    client.status.send("VNA_COMPLETE")
    level, status = server.status_reader.read()
    assert status == "VNA_COMPLETE"

    client.status.send("VNA_ERROR")
    level, status = server.status_reader.read()
    assert status == "VNA_ERROR"

    client.status.send("VNA_TIMEOUT")
    level, status = server.status_reader.read()
    assert status == "VNA_TIMEOUT"
