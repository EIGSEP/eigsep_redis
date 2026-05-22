import logging

from .keys import STATUS_STREAM
from .single_stream import SingleStreamReader, SingleStreamWriter

logger = logging.getLogger(__name__)


class StatusWriter(SingleStreamWriter):
    """
    Publish status messages onto the status stream.

    Producers call ``send(level, status)`` to emit a human-readable
    status line tagged with a Python logging level. The stream is
    bounded via ``maxlen`` so a dead consumer can't grow it without
    limit. The bound is sized to survive a brief ground-reader
    outage without dropping diverse event types — the durable record
    of every event remains the panda's rotating log file.
    """

    stream = STATUS_STREAM
    # Status is a singleton with no registry-set; nothing to SADD.
    data_set = None
    maxlen = 100

    def _encode(self, status, level=logging.INFO):
        return {"level": level, "status": status}

    def send(self, status, level=logging.INFO):
        """
        Publish a status message.

        Parameters
        ----------
        status : str
            Status message.
        level : int
            Python logging level.
        """
        self.publish(status, level=level)


class StatusReader(SingleStreamReader):
    """
    Consume status messages from the status stream.

    ``read`` is a blocking XREAD scoped to ``stream:status`` only; it
    cannot be coerced to read any other stream (the stream name is
    hard-coded in :data:`STATUS_STREAM`).
    """

    stream = STATUS_STREAM
    # Status is a singleton with no registry-set; skip the
    # membership check that other buses use.
    data_set = None

    def _timeout_value(self):
        """Status read returns ``(None, None)`` on timeout instead of
        raising so the consumer loop can poll without try/except."""
        return None, None

    def _decode(self, entry_id, fields):
        status = fields.get(b"status").decode("utf-8")
        raw_level = fields.get(b"level")
        if raw_level is None:
            level = logging.INFO
        else:
            level = int(raw_level.decode("utf-8"))
        return level, status
