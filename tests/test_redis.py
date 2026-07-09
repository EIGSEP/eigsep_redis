"""Unit tests for the eigsep_redis bus primitives.

Pure-package tests covering the Transport and writer/reader surfaces.
End-to-end integration tests that exercise corr / VNA alongside
metadata live in ``eigsep_observing/tests/test_redis.py`` — which
imports both packages — and must stay green when this package is
upgraded.
"""

import json
import logging
import socket
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
    entry_id_to_unix,
)
from eigsep_redis.keys import METADATA_HASH
from eigsep_redis.testing import DummyTransport


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

    def set_last_read_id(self, stream, read_id):
        return self.transport.set_last_read_id(stream, read_id)

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
    server.set_last_read_id("stream:acc_cnt", "0-0")
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


def test_transport_passes_connect_timeout_to_redis(monkeypatch):
    """Transport must give the redis client a finite ``socket_connect_timeout``
    so the initial ``ping()`` fails fast on an unreachable host instead of
    blocking on the OS-level TCP timeout (~2 min on Linux).

    ``socket_timeout`` must stay ``None`` so blocking reads (``XREAD BLOCK``,
    used by every stream reader) don't spuriously time out.
    """
    from eigsep_redis.transport import Transport

    captured = {}

    class _FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def ping(self):
            return True

    monkeypatch.setattr("eigsep_redis.transport.redis.Redis", _FakeClient)

    Transport(host="example.invalid", port=6379, connect_timeout=2.5)

    assert captured["socket_connect_timeout"] == 2.5
    assert captured["socket_timeout"] is None


def test_transport_eager_default_pings(monkeypatch):
    """Default ``lazy=False`` Transport calls ``ping()`` during ``__init__``
    and raises ``ConnectionError`` on an unreachable server — the
    backwards-compatible fail-fast contract that every existing consumer
    (picohost, eigsep_observing, etc.) depends on.
    """
    import redis.exceptions

    from eigsep_redis.transport import Transport

    pings = []

    class _FakeClient:
        def __init__(self, **kwargs):
            pass

        def ping(self):
            pings.append(True)
            raise redis.exceptions.ConnectionError("nope")

    monkeypatch.setattr("eigsep_redis.transport.redis.Redis", _FakeClient)

    with pytest.raises(redis.exceptions.ConnectionError):
        Transport(host="example.invalid", port=6379)

    assert pings == [True]


def test_transport_lazy_skips_ping(monkeypatch):
    """``lazy=True`` Transport must not ping during ``__init__`` so it
    succeeds against an unreachable server. The lazy mode exists for the
    opportunistic-peer pattern in ``eigsep_observing.scripts.observe``:
    construction always succeeds and connection failures surface at the
    first read/write instead.
    """
    from eigsep_redis.transport import Transport

    pings = []

    class _FakeClient:
        def __init__(self, **kwargs):
            pass

        def ping(self):
            # Intentionally count — we assert the lazy path never
            # reaches here at construction time.
            pings.append(True)
            return True

    monkeypatch.setattr("eigsep_redis.transport.redis.Redis", _FakeClient)

    t = Transport(host="example.invalid", port=6379, lazy=True)

    assert pings == []
    assert t.lazy is True


def test_transport_lazy_is_connected_uses_ping(monkeypatch):
    """``is_connected()`` is the explicit health check for lazy callers:
    a single ping that returns ``True``/``False`` instead of raising.
    Pairs with ``lazy=True`` to give consumers an opt-in probe before
    issuing a real read.
    """
    import redis.exceptions

    from eigsep_redis.transport import Transport

    state = {"alive": False}

    class _FakeClient:
        def __init__(self, **kwargs):
            pass

        def ping(self):
            if not state["alive"]:
                raise redis.exceptions.ConnectionError("nope")
            return True

    monkeypatch.setattr("eigsep_redis.transport.redis.Redis", _FakeClient)

    t = Transport(host="example.invalid", port=6379, lazy=True)
    assert t.is_connected() is False
    state["alive"] = True
    assert t.is_connected() is True


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
            {
                "drain",
                "skip_to_latest",
                "streams",
                "max_age_s",
                "warn_interval_s",
            },
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
    # Status* are now SingleStreamReader/SingleStreamWriter subclasses.
    # ``vars(cls)`` only enumerates names defined on the subclass
    # itself (not inherited), so the expected sets here are the
    # bus-specific knobs each Status class adds on top of the base.
    # Inherited methods (``read``, ``publish``) still work; this guard
    # exists to catch cross-bus methods being added to a class, not
    # to assert the inheritance surface.
    surfaces = {
        StatusWriter: {"send", "maxlen", "stream", "data_set"},
        StatusReader: {"stream", "data_set"},
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


def test_metadata_drain_skips_producer_backlog(server, client):
    """Tier-2 guard for metadata streams: a consumer whose cache is
    empty must see a producer-first backlog as "past" and return an
    empty drain, not replay the backlog.
    """
    for i in range(5):
        client.metadata.add("acc_cnt", i)
    assert server.metadata_stream.drain() == {}


def test_metadata_drain_catches_entries_after_first_drain(server, client):
    """Regression for issue #13: the first drain on a fresh cache
    skips backlog (documented behavior), but every subsequent drain
    must capture entries the producer publishes between drains —
    without requiring an explicit ``skip_to_latest()`` first.

    Before the fix, ``_streams_from_set`` resolved the cursor via
    ``xinfo_stream`` but never wrote it back into ``_last_read_ids``,
    so each drain re-seeked to the current tail and entries published
    between drains were lost.
    """
    client.metadata.add("acc_cnt", 1)
    assert server.metadata_stream.drain() == {}  # backlog skipped
    client.metadata.add("acc_cnt", 2)
    client.metadata.add("acc_cnt", 3)
    assert server.metadata_stream.drain() == {"stream:acc_cnt": [2, 3]}


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


def test_named_heartbeats_are_isolated(server, client):
    """Multiple named heartbeats on the same transport must not
    collide. Default ``name="client"`` coexists with per-device names
    (e.g. ``pico:motor``) because each one writes to its own
    ``heartbeat:{name}`` key.
    """
    motor_w = HeartbeatWriter(client.transport, name="pico:motor")
    motor_r = HeartbeatReader(server.transport, name="pico:motor")
    imu_w = HeartbeatWriter(client.transport, name="pico:imu_el")
    imu_r = HeartbeatReader(server.transport, name="pico:imu_el")

    assert motor_w.key == "heartbeat:pico:motor"
    assert motor_r.key == "heartbeat:pico:motor"

    motor_w.set(ex=100, alive=True)
    assert motor_r.check() is True
    # Default-named reader must not see the per-device heartbeat
    assert server.heartbeat_reader.check() is False
    # IMU heartbeat is independent of motor's
    assert imu_r.check() is False

    imu_w.set(ex=100, alive=True)
    assert motor_r.check() is True
    assert imu_r.check() is True

    motor_w.set(ex=100, alive=False)
    assert motor_r.check() is False
    assert imu_r.check() is True


def test_status(server, client):
    # StatusReader.stream is the stream name (str) inherited from
    # SingleStreamReader; an unread, never-existed status stream has
    # no cached cursor, so the transport falls back to the "$"
    # sentinel (newest-after-call).
    assert client.status_reader.stream == "stream:status"
    assert (
        client.transport.get_last_read_id(client.status_reader.stream) == "$"
    )

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


def test_status_read_catches_entries_after_empty_read(server, client):
    """Regression for the symmetric cursor-cache bug in
    ``Transport.get_last_read_id``: an empty (timeout) read on a
    stream that already has entries must seed the cursor, so the next
    read picks up entries the producer publishes between calls
    instead of re-seeking to the new tail and dropping them.
    """
    client.status.send("backlog")
    # Short timeout — backlog skipped by design, no new entries arrive
    # during the block window, so read returns (None, None).
    level, status = server.status_reader.read(timeout=0.05)
    assert (level, status) == (None, None)
    client.status.send("fresh")
    level, status = server.status_reader.read(timeout=0.05)
    assert status == "fresh"


def test_entry_id_to_unix_bytes_and_str():
    """Redis stream entry IDs are ``{millis}-{seq}``; helper drops the
    sequence and returns Unix seconds as a float. Accepts both bytes
    (xread's return shape) and str."""
    assert entry_id_to_unix(b"1700000000000-0") == 1.7e9
    assert entry_id_to_unix("1700000000000-7") == 1.7e9
    # sub-second resolution is preserved
    assert entry_id_to_unix(b"1700000000123-0") == pytest.approx(
        1.700000000123e9
    )


def test_metadata_drain_with_ids_returns_tuples(server, client):
    """drain(with_ids=True) returns (entry_id, value) tuples and
    advances the read pointer exactly like the default shape, so the
    next call returns empty until new entries arrive."""
    # Register the stream, then anchor the reader at the start of the
    # stream so the backlog-skip default doesn't hide the entries
    # under test. (Mirrors how the corr-loop side ends up positioned
    # once it has consumed at least one entry.)
    client.metadata.add("acc_cnt", 0)
    server.transport.set_last_read_id("stream:acc_cnt", "0-0")
    client.metadata.add("acc_cnt", 1)

    result = server.metadata_stream.drain(with_ids=True)
    assert set(result.keys()) == {"stream:acc_cnt"}
    entries = result["stream:acc_cnt"]
    assert len(entries) == 2
    for eid, value in entries:
        assert isinstance(eid, bytes)
        # entry IDs parse as Unix seconds matching wall clock
        assert abs(entry_id_to_unix(eid) - time.time()) < 5.0
    assert [v for _, v in entries] == [0, 1]

    # pointer advanced — next drain is empty until new entries arrive
    assert server.metadata_stream.drain(with_ids=True) == {}

    client.metadata.add("acc_cnt", 2)
    follow = server.metadata_stream.drain(with_ids=True)
    assert [v for _, v in follow["stream:acc_cnt"]] == [2]


def test_metadata_skip_to_latest_advances_past_backlog(server, client):
    """Consumer that has been reading a stream calls skip_to_latest
    after a hypothetical transport blip — pending unread entries are
    discarded, and only entries produced after the skip are returned."""
    # Anchor the reader at the start so we can verify entry 0 is
    # consumed normally before the simulated blip.
    client.metadata.add("acc_cnt", 0)
    server.transport.set_last_read_id("stream:acc_cnt", "0-0")
    assert server.metadata_stream.drain() == {"stream:acc_cnt": [0]}

    # producer keeps publishing during the simulated outage
    for i in range(1, 4):
        client.metadata.add("acc_cnt", i)

    server.metadata_stream.skip_to_latest("stream:acc_cnt")

    # entries 1..3 are discarded; only post-skip entries surface
    client.metadata.add("acc_cnt", 99)
    assert server.metadata_stream.drain() == {"stream:acc_cnt": [99]}


def test_metadata_skip_to_latest_defaults_to_registered_streams(
    server, client
):
    """With no argument, skip_to_latest walks METADATA_STREAMS_SET and
    advances every registered stream."""
    client.metadata.add("acc_cnt", 0)
    client.metadata.add("temp", 25.0)
    server.transport.set_last_read_id("stream:acc_cnt", "0-0")
    server.transport.set_last_read_id("stream:temp", "0-0")
    # anchor + drain entry 0 on each stream
    assert server.metadata_stream.drain() == {
        "stream:acc_cnt": [0],
        "stream:temp": [25.0],
    }

    # backlog on both streams
    for i in range(1, 3):
        client.metadata.add("acc_cnt", i)
        client.metadata.add("temp", 25.0 + i)

    server.metadata_stream.skip_to_latest()

    client.metadata.add("acc_cnt", 99)
    client.metadata.add("temp", 100.0)
    drained = server.metadata_stream.drain()
    assert drained == {
        "stream:acc_cnt": [99],
        "stream:temp": [100.0],
    }


def test_metadata_skip_to_latest_unknown_stream_is_noop(server, client):
    """Calling skip_to_latest on a stream that doesn't exist yet is a
    no-op: it must not raise, and must not cache ``$`` (or any other
    value) in ``_last_read_ids``. Caching ``$`` would freeze the
    pointer at a value that never matches a real entry; leaving
    ``_last_read_ids`` untouched lets the standard fallback path take
    over once the producer creates the stream."""
    server.metadata_stream.skip_to_latest("stream:not_yet")
    assert "stream:not_yet" not in server.transport._last_read_ids
    # And it doesn't crash when none of the requested streams exist
    server.metadata_stream.skip_to_latest(["stream:a", "stream:b", "stream:c"])
    assert server.transport._last_read_ids == {}


def test_transport_sets_tcp_keepalive():
    """Issue #23: a peer that power-cuts mid-conversation leaves a
    half-open TCP connection; with socket_timeout=None (deliberate —
    finite timeouts break block=0 XREADs) an in-flight recv() blocks
    forever. Kernel TCP keepalive is the guard: assert the options
    land on the connection pool. 192.0.2.1 is TEST-NET (never
    routable); lazy=True skips the startup ping so no connection is
    attempted.
    """
    from eigsep_redis.transport import Transport

    t = Transport(host="192.0.2.1", port=6379, lazy=True)
    kwargs = t.r.connection_pool.connection_kwargs
    assert kwargs["socket_keepalive"] is True
    expected = {
        getattr(socket, name): val
        for name, val in (
            ("TCP_KEEPIDLE", 30),
            ("TCP_KEEPINTVL", 10),
            ("TCP_KEEPCNT", 3),
        )
        if hasattr(socket, name)
    }
    assert expected, "platform exposes no TCP keepalive constants"
    assert kwargs["socket_keepalive_options"] == expected
