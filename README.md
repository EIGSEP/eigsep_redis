# eigsep_redis

Redis transport and bus surfaces for the
[EIGSEP](https://github.com/EIGSEP) radio astronomy experiment.

Provides the `Transport` object (Redis connection + last-read-id
bookkeeping) and per-bus writer/reader classes (`MetadataWriter`,
`MetadataSnapshotReader`, `MetadataStreamReader`, `StatusWriter`,
`StatusReader`, `HeartbeatWriter`, `HeartbeatReader`, `ConfigStore`)
used by the observing stack and by the picohost producer library.

Split out of `eigsep_observing` so that producers (e.g. `picohost`)
can depend on just the bus primitives without pulling in the full
observing stack (`h5py`, `flask`, `eigsep-vna`, etc.).

## Installation

```bash
pip install -e ".[dev]"
```

## Development

```bash
pytest                   # tests + coverage
ruff check .             # lint
ruff format --check .    # formatting (line length 79)
```

## Testing

Tests use `fakeredis` via `DummyTransport` — no Redis server required.
The full producer↔consumer integration tests live in
[`eigsep_observing`](https://github.com/EIGSEP/eigsep_observing)
(`tests/test_redis.py`), which imports both packages.

## Related repos

- [`eigsep_observing`](https://github.com/EIGSEP/eigsep_observing) —
  observation pipeline that consumes this bus.
- `picohost` — Pico microcontroller producer library that publishes to
  this bus.
