# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
pip install -e ".[dev]"        # install (Python >=3.10)
pytest                         # tests + coverage (60s per-test timeout)
pytest tests/test_redis.py::test_name   # single test
ruff check .                   # lint
ruff format --check .          # formatting (line length 79)
```

Tests use `fakeredis` via `eigsep_redis.testing.DummyTransport` — no Redis server is required locally. End-to-end integration tests that exercise corr / VNA alongside metadata live in `eigsep_observing/tests/test_redis.py` (separate repo) and must stay green when this package is upgraded.

## Architecture

This package was split out of `eigsep_observing` so that producers (e.g. `picohost`) can depend on just the bus primitives without pulling in `h5py`, `flask`, `eigsep-vna`, etc. It exposes a shared `Transport` and paired writer/reader classes — one pair per logical bus.

**`Transport` (transport.py).** Owns the Redis connection, the per-stream `last-read-id` bookkeeping, and raw K/V helpers. Every writer/reader takes a `Transport` and shares state through it. Subclass and override `_make_redis` to swap the client — `testing/transport.py::DummyTransport` is the fakeredis subclass used by the test suite.

**Bus surfaces.** Each is a thin Writer/Reader pair over the `Transport`:

- `metadata.py` — `MetadataWriter` writes each `add(key, value)` to *both* a live hash (`METADATA_HASH`) *and* a per-key stream (`stream:{key}`) in one logical op, and registers the stream in `METADATA_STREAMS_SET` / `DATA_STREAMS_SET`. This dual-write is load-bearing: splitting it lets the two readers drift. `MetadataSnapshotReader` reads the live hash (point-in-time, used by VNA); `MetadataStreamReader.drain()` advances per-stream pointers and returns everything since the last call (cadence-matched averaging, used by the corr loop). Both readers warn on stale `{key}_ts` timestamps — snapshot warns per `get()`, stream warns per-stream throttled by `warn_interval_s` so a dead sensor doesn't spam at corr cadence (~4 Hz).
- `status.py` — `StatusWriter.send(status, level)` / `StatusReader.read(timeout)` on the singleton `STATUS_STREAM`. Bounded `maxlen=100`; durable event record lives in the panda's log file.
- `heartbeat.py` — per-name liveness key at `heartbeat:{name}`. Default `name="client"` is the panda-side `PandaClient` heartbeat; `picohost.manager` passes device-specific names (`"pico:motor"`, `"pico:imu_el"`) so one transport carries many heartbeats without collisions.
- `config.py` — `ConfigStore.upload(dict)` / `get()` on the single `CONFIG_KEY`. Dict-only by design — YAML parsing is the caller's responsibility, keeping the store independent of `eigsep_observing.utils`. The SNAP-side correlator config is a separate store in `eigsep_observing`.

**`keys.py` is the authoritative key registry.** Every key/stream/set constant lives there so collisions are visible at import. Observer-side keys (corr, vna) live in `eigsep_observing.keys`; both modules are checked for cross-package uniqueness by a test in `eigsep_observing`. When adding a new bus, add its key constant here — not inline in the writer.

**Composition is the caller's job.** There is no "bus bundle" class in `src/`. Each role in `eigsep_observing` (`EigObserver`, `PandaClient`, `EigsepFpga`) instantiates only the writer/reader pairs it needs over a shared `Transport`. The test suite builds a `_BusBundle` locally for round-trip tests — do not re-introduce it into `src/`.

## Release

Versioning is managed by release-please (`release-please-config.json`, `.release-please-manifest.json`). Use Conventional Commits (`feat:`, `fix:`, `refactor!:` for breaking). The `release-please` workflow opens a release PR; merging it tags and publishes. Do not hand-edit `pyproject.toml` version or `CHANGELOG.md`.
