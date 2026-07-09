"""
Unit tests for :class:`SingleStreamReader` / :class:`SingleStreamWriter`.

Exercises the base classes through a toy subclass that encodes/decodes
a single ``value`` field, so the tests stay focused on the
blocking-XREAD + cursor-bookkeeping + registry-membership behavior the
base classes own (concrete bus tests cover their own decode logic).
"""

import logging

import pytest

from eigsep_redis import SingleStreamReader, SingleStreamWriter
from eigsep_redis.keys import DATA_STREAMS_SET
from eigsep_redis.testing import DummyTransport


TOY_STREAM = "stream:toy"


class ToyWriter(SingleStreamWriter):
    stream = TOY_STREAM
    maxlen = 100

    def _encode(self, value):
        return {"value": str(value).encode("utf-8")}


class ToyReader(SingleStreamReader):
    stream = TOY_STREAM
    absent_warning = "no toy stream yet"

    def _decode(self, entry_id, fields):
        return int(fields[b"value"].decode("utf-8"))


class TupleReader(SingleStreamReader):
    """Demonstrates overriding the absent / timeout return shape."""

    stream = TOY_STREAM

    def _decode(self, entry_id, fields):
        return entry_id, int(fields[b"value"].decode("utf-8"))

    def _absent_sentinel(self):
        return None, None

    def _timeout_value(self):
        return None, None


class UnregisteredReader(SingleStreamReader):
    """Singleton-stream reader (no membership check) — like StatusReader."""

    stream = TOY_STREAM
    data_set = None

    def _decode(self, entry_id, fields):
        return int(fields[b"value"].decode("utf-8"))


@pytest.fixture
def server():
    return DummyTransport()


@pytest.fixture
def client(server):
    t = DummyTransport()
    t.r = server.r
    return t


def test_round_trip(server, client):
    """publish() → read() recovers the encoded value."""
    writer = ToyWriter(client)
    reader = ToyReader(server)
    # anchor at 0-0 so the first read returns the just-published entry
    # rather than the default "skip backlog" sentinel — same pattern
    # downstream tests use.
    writer.publish(42)
    server.set_last_read_id(TOY_STREAM, "0-0")
    assert reader.read(timeout=0.1) == 42


def test_writer_registers_stream_in_data_set(server, client):
    """Default writer SADD's the stream into DATA_STREAMS_SET so the
    matching reader's membership check passes."""
    assert server.r.smembers(DATA_STREAMS_SET) == set()
    ToyWriter(client).publish(1)
    assert server.r.smembers(DATA_STREAMS_SET) == {TOY_STREAM.encode()}


def test_reader_absent_sentinel_when_unregistered(server, caplog):
    """If the stream isn't in data_set, read() returns the sentinel
    and (when absent_warning is set) logs a warning. It does NOT
    block on xread — the membership check is the short-circuit."""
    reader = ToyReader(server)
    with caplog.at_level(logging.WARNING, logger="eigsep_redis.transport"):
        result = reader.read(timeout=0.05)
    assert result is None
    assert any("no toy stream yet" in r.message for r in caplog.records)


def test_reader_absent_sentinel_tuple_override(server):
    """Subclasses can return a tuple sentinel by overriding
    _absent_sentinel."""
    assert TupleReader(server).read(timeout=0.05) == (None, None)


def test_reader_no_check_when_data_set_is_none(server, client):
    """data_set=None skips the membership check — the reader will
    block on xread directly. Mirrors StatusReader's singleton case."""
    # publish via raw xadd so the stream exists in Redis but is
    # *not* registered in DATA_STREAMS_SET. A reader with
    # data_set=DATA_STREAMS_SET would short-circuit; UnregisteredReader
    # bypasses the check and reads the entry.
    server.r.xadd(TOY_STREAM, {"value": b"7"})
    reader = UnregisteredReader(server)
    reader.transport.set_last_read_id(TOY_STREAM, "0-0")
    assert reader.read(timeout=0.1) == 7


def test_first_entry_after_absent_poll_is_delivered(server, client):
    """Regression for the fresh-stream first-entry swallow: a reader
    that polled while the stream was absent must receive the entry
    whose publish created and registered the stream, instead of
    lazily initializing its cursor at that entry's id and skipping
    it.

    Field case (2026-07-09): live_status polled ``stream:vna`` on a
    fresh post-reboot Redis; the first VNA bundle registered the
    stream and was silently swallowed, the second one painted. At
    ~1/hour cadence that loses a real measurement, not a tick.
    """
    reader = ToyReader(server)
    # consumer polls before any producer has ever written
    assert reader.read(timeout=0.05) is None
    # producer's first-ever publish creates + registers the stream
    ToyWriter(client).publish(42)
    # the next poll must deliver that first entry, not skip it
    assert reader.read(timeout=0.1) == 42


def test_reader_timeout_default_raises(server, client):
    """Default _timeout_value raises TimeoutError — what corr/vna/adc want."""
    ToyWriter(client).publish(1)  # registers in data_set
    server.set_last_read_id(TOY_STREAM, "$")  # skip the entry above
    with pytest.raises(TimeoutError, match=TOY_STREAM):
        ToyReader(server).read(timeout=0.05)


def test_reader_timeout_override_returns_sentinel(server, client):
    """Subclasses can return a sentinel on timeout — what Status wants."""
    ToyWriter(client).publish(1)
    server.set_last_read_id(TOY_STREAM, "$")
    assert TupleReader(server).read(timeout=0.05) == (None, None)


def test_cursor_advances_across_reads(server, client):
    """Regression for issue #13's symmetric case: after each read,
    the cursor is cached so the next read picks up the next entry
    instead of re-seeking to the tail and dropping in-flight entries.
    """
    writer = ToyWriter(client)
    reader = ToyReader(server)
    writer.publish(1)
    server.set_last_read_id(TOY_STREAM, "0-0")
    assert reader.read(timeout=0.1) == 1

    # Publish more entries; reader should pick up each in order.
    writer.publish(2)
    writer.publish(3)
    assert reader.read(timeout=0.1) == 2
    assert reader.read(timeout=0.1) == 3


def test_writer_data_set_none_skips_registration(server, client):
    """A subclass that sets data_set=None publishes without
    registering the stream — singleton bus pattern."""

    class SingletonWriter(SingleStreamWriter):
        stream = TOY_STREAM
        data_set = None

        def _encode(self, value):
            return {"value": str(value).encode("utf-8")}

    SingletonWriter(client).publish(5)
    assert server.r.smembers(DATA_STREAMS_SET) == set()
    # but the entry IS in the stream
    assert server.r.xlen(TOY_STREAM) == 1


def test_read_blocks_indefinitely_with_none_timeout(server, client):
    """timeout=None means block=0 (forever in Redis terms); verify by
    publishing an entry and confirming the read picks it up rather
    than returning a sentinel. Done synchronously since fakeredis
    handles the round-trip in-process."""
    writer = ToyWriter(client)
    reader = ToyReader(server)
    writer.publish(9)
    server.set_last_read_id(TOY_STREAM, "0-0")
    # No real blocking on fakeredis — but a present entry must be
    # returned regardless of the timeout encoding.
    assert reader.read(timeout=None) == 9


def test_base_classes_raise_when_subclass_skips_encode_decode(server):
    """Bare SingleStreamReader / Writer (or a subclass that forgets
    _decode / _encode) must fail loudly, not silently no-op."""
    server.r.xadd(TOY_STREAM, {"value": b"1"})
    server.r.sadd(DATA_STREAMS_SET, TOY_STREAM)
    server.set_last_read_id(TOY_STREAM, "0-0")

    class BareReader(SingleStreamReader):
        stream = TOY_STREAM

    with pytest.raises(NotImplementedError):
        BareReader(server).read(timeout=0.1)

    class BareWriter(SingleStreamWriter):
        stream = TOY_STREAM

    with pytest.raises(NotImplementedError):
        BareWriter(server).publish(1)
