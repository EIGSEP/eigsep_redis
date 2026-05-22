import json
import logging
import threading
import time

import redis
import redis.exceptions

logger = logging.getLogger(__name__)


class Transport:
    """
    Shared Redis transport: connection, last-read-id bookkeeping,
    raw K/V, and lifecycle.

    Owns nothing bus-specific. Writer and reader classes are
    constructed with a ``Transport`` and share the connection and
    last-read-id state through it. Subclass and override
    ``_make_redis`` to swap the underlying client (e.g. fakeredis
    for testing).
    """

    def __init__(
        self,
        host="localhost",
        port=6379,
        connect_timeout=5.0,
        lazy=False,
    ):
        """Construct a Transport.

        Parameters
        ----------
        host, port : str, int
            Redis server address.
        connect_timeout : float
            ``socket_connect_timeout`` for the underlying ``redis.Redis``
            client. Bounds how long the eager ping (when ``lazy=False``)
            and any reconnect attempt will block before raising.
        lazy : bool
            If ``False`` (default), ping the server during ``__init__``
            and raise ``redis.exceptions.ConnectionError`` if the server
            is unreachable — the original fail-fast behavior. If
            ``True``, build the client but skip the ping; construction
            always succeeds, and connection failures surface at the
            first read/write instead. Use ``lazy=True`` when the
            transport is for an opportunistic peer that may be offline
            at startup but should be picked up implicitly once it
            recovers (e.g. the LattePanda from
            ``eigsep_observing.scripts.observe``). Callers that need an
            explicit health check can use :meth:`is_connected`.
        """
        self.logger = logger
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.lazy = lazy
        self._stream_lock = threading.RLock()
        self._last_read_ids = {}
        self.r = self._make_redis(host, port)

    def _make_redis(self, host, port):
        try:
            r = redis.Redis(
                host=host,
                port=port,
                decode_responses=False,
                socket_timeout=None,
                socket_connect_timeout=self.connect_timeout,
                retry_on_timeout=False,
            )
            if self.lazy:
                self.logger.info(
                    f"Built lazy Redis client for {host}:{port} "
                    "(no startup ping; failures surface at first use)."
                )
            else:
                r.ping()
                self.logger.info(f"Connected to Redis at {host}:{port}")
        except redis.exceptions.ConnectionError as e:
            self.logger.error(
                f"Failed to connect to Redis at {host}:{port}: {e}"
            )
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to Redis: {e}")
            raise
        return r

    def _get_last_read_id(self, stream):
        with self._stream_lock:
            if stream in self._last_read_ids:
                return self._last_read_ids[stream]
            try:
                last_id = self.r.xinfo_stream(stream)["last-generated-id"]
            except redis.exceptions.ResponseError:
                # Stream doesn't exist yet; "$" is a per-call xread
                # sentinel ("tail at call time"), not a stable ID, so
                # leave the cache empty until a real ID materializes.
                return "$"
            self._last_read_ids[stream] = last_id
            return last_id

    def _set_last_read_id(self, stream, read_id):
        with self._stream_lock:
            self._last_read_ids[stream] = read_id

    def _streams_from_set(self, set_name):
        """
        Build a ``{stream_name: last_read_id}`` dict from a Redis set
        of stream names. Missing entries default to the last generated
        ID if available, otherwise falls back to '$' (newest after read).
        """
        members = self.r.smembers(set_name)
        with self._stream_lock:
            d = {}
            for s in members:
                key = s.decode()
                if key in self._last_read_ids:
                    d[key] = self._last_read_ids[key]
                    continue
                try:
                    last_id = self.r.xinfo_stream(key)["last-generated-id"]
                except redis.exceptions.ResponseError:
                    # See _get_last_read_id: "$" is a sentinel, not a
                    # cacheable ID — leave the cache empty.
                    d[key] = "$"
                    continue
                d[key] = last_id
                self._last_read_ids[key] = last_id
            return d

    def reset(self):
        """Flush the whole Redis DB and reset last-read-id state."""
        self.r.flushdb()
        with self._stream_lock:
            self._last_read_ids.clear()

    def add_raw(self, key, value, ex=None):
        return self.r.set(key, value, ex=ex)

    def get_raw(self, key):
        return self.r.get(key)

    def upload_dict(self, d, key):
        """Serialize ``d`` as JSON (with ``upload_time`` injected) under ``key``."""
        d = d.copy()
        d["upload_time"] = time.time()
        self.add_raw(key, json.dumps(d).encode("utf-8"))

    def is_connected(self):
        try:
            return self.r.ping()
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):
            return False

    def close(self):
        try:
            if hasattr(self.r, "close"):
                self.r.close()
            self.logger.info("Redis connection closed")
        except Exception as e:
            self.logger.warning(f"Error closing Redis connection: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
