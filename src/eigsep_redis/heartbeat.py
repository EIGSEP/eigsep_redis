class HeartbeatWriter:
    """
    Set/clear a named liveness heartbeat under ``heartbeat:{name}``.

    The panda-side ``PandaClient`` uses the default ``name="client"``
    to publish at ~1 Hz with a short TTL so a crashed client is
    detected within a bounded window; the ground-side observer reads
    it via :class:`HeartbeatReader.check`.

    Other writers — notably ``picohost.manager`` — pass their own
    ``name`` (e.g. ``"pico:motor"``, ``"pico:imu_el"``) so a single
    transport can carry per-device heartbeats without collisions.
    """

    def __init__(self, transport, name="client"):
        self.transport = transport
        self.name = name
        self.key = f"heartbeat:{name}"

    def set(self, ex=None, alive=True):
        """
        Publish a heartbeat tick.

        Parameters
        ----------
        ex : int or None
            Optional TTL in seconds. Typical pattern: set with
            ``ex=60`` on a ~1 Hz cadence so a crashed client is
            detected within 60s.
        alive : bool
            ``True`` to mark the client alive, ``False`` to mark it
            down (shutdown).
        """
        self.transport.add_raw(self.key, int(alive), ex=ex)


class HeartbeatReader:
    """Read-only view of a named liveness heartbeat."""

    def __init__(self, transport, name="client"):
        self.transport = transport
        self.name = name
        self.key = f"heartbeat:{name}"

    def check(self):
        """
        Return ``True`` if the heartbeat is alive, ``False`` otherwise.

        A missing key (TTL expired, never set) returns ``False``.
        """
        raw = self.transport.get_raw(self.key)
        if raw is None:
            return False
        return int(raw) == 1
