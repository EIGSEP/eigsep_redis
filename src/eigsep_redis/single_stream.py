"""
Reusable single-stream blocking read/write primitives.

Per-bus readers and writers (corr / vna / adc / status / …) share the
same blocking-XREAD + cursor-bookkeeping shape on the read side and the
same XADD + registry-set bookkeeping shape on the write side. The base
classes below capture that shape so concrete classes only have to
declare the stream name and implement the bus-specific decode/encode
step.

Concrete examples in this package: :class:`~eigsep_redis.StatusWriter`
and :class:`~eigsep_redis.StatusReader`. Downstream readers/writers in
``eigsep_observing`` (``CorrReader``, ``VnaReader``,
``AdcSnapshotReader`` and their writer counterparts) subclass these.
"""

import logging

from .keys import DATA_STREAMS_SET

logger = logging.getLogger(__name__)


class SingleStreamReader:
    """
    Blocking single-stream reader with cursor bookkeeping.

    Subclasses set :attr:`stream` (the Redis stream name) and
    optionally :attr:`data_set` (the registry-set name to
    membership-check before reading; ``None`` skips the check — used
    by readers whose stream is a known singleton with no registry,
    e.g. ``StatusReader``). Subclasses implement
    :meth:`_decode` to turn the raw ``(entry_id, fields)`` shape
    returned by Redis into whatever the consumer wants.

    Subclasses can override :meth:`_timeout_value` and
    :meth:`_absent_sentinel` to control the return shape on timeout
    and on missing-stream respectively — the defaults raise
    ``TimeoutError`` and return ``None``.
    """

    stream: str
    data_set: str | None = DATA_STREAMS_SET
    absent_warning: str | None = None

    def __init__(self, transport):
        self.transport = transport

    def read(self, timeout=None):
        """
        Blocking read of the next entry on :attr:`stream`.

        Parameters
        ----------
        timeout : float or None
            Block timeout in seconds. ``None`` blocks indefinitely.

        Returns
        -------
        Whatever :meth:`_decode` returns, or
        :meth:`_absent_sentinel` if a membership check on
        :attr:`data_set` fails, or whatever :meth:`_timeout_value`
        returns (default: raises ``TimeoutError``) on an empty read.
        """
        r = self.transport.r
        if self.data_set is not None and not r.sismember(
            self.data_set, self.stream
        ):
            if self.absent_warning:
                self.transport.logger.warning(self.absent_warning)
            # A stream observed absent has no backlog to skip: pin the
            # cursor to "0" so the entry whose publish creates and
            # registers the stream is delivered on the next read,
            # instead of being swallowed by get_last_read_id's lazy
            # last-generated-id initialization. Skip-to-latest for
            # streams that already exist at first read is unchanged.
            self.transport.set_last_read_id(self.stream, "0")
            return self._absent_sentinel()
        last_id = self.transport.get_last_read_id(self.stream)
        block_ms = 0 if timeout is None else int(timeout * 1000)
        out = r.xread({self.stream: last_id}, count=1, block=block_ms)
        if not out:
            return self._timeout_value()
        eid, fields = out[0][1][0]
        self.transport.set_last_read_id(self.stream, eid)
        return self._decode(eid, fields)

    def _decode(self, entry_id, fields):
        """Turn a raw ``(entry_id, fields)`` pair into the consumer
        return value. ``fields`` is the ``{bytes: bytes}`` dict that
        ``xread`` produces. Subclass must implement."""
        raise NotImplementedError

    def _absent_sentinel(self):
        """Return value when the stream isn't registered in
        :attr:`data_set`. Default ``None``; subclasses with a
        tuple-shaped return type (e.g. ``(None, None)``) should
        override."""
        return None

    def _timeout_value(self):
        """Return value (or raise) when the blocking read times out.
        Default raises ``TimeoutError``; subclasses that prefer a
        sentinel value (e.g. ``StatusReader`` returning
        ``(None, None)``) should override."""
        raise TimeoutError(f"No data on {self.stream!r} within timeout.")


class SingleStreamWriter:
    """
    XADD + optional registry-set SADD in one shot.

    Subclasses set :attr:`stream`, optionally :attr:`data_set`
    (default :data:`DATA_STREAMS_SET`; ``None`` skips registration —
    used by writers whose stream needs no registry, e.g.
    ``StatusWriter``), :attr:`maxlen`, and :attr:`approximate`.
    Subclasses implement :meth:`_encode` to turn caller arguments
    into the ``{bytes: bytes}`` payload XADD expects.

    Subclasses that need pre/post hooks around the XADD (e.g.
    ``CorrWriter`` also registers per-pair keys in a side set before
    publishing) can override :meth:`publish` and call
    ``super().publish(...)`` from their own implementation.
    """

    stream: str
    data_set: str | None = DATA_STREAMS_SET
    maxlen: int | None = None
    approximate: bool = True

    def __init__(self, transport):
        self.transport = transport

    def publish(self, *args, **kwargs):
        """
        Encode caller arguments and XADD them to :attr:`stream`,
        registering the stream in :attr:`data_set` if set.
        """
        payload = self._encode(*args, **kwargs)
        r = self.transport.r
        r.xadd(
            self.stream,
            payload,
            maxlen=self.maxlen,
            approximate=self.approximate,
        )
        if self.data_set is not None:
            r.sadd(self.data_set, self.stream)

    def _encode(self, *args, **kwargs):
        """Turn caller arguments into the XADD payload dict.
        Subclass must implement."""
        raise NotImplementedError
