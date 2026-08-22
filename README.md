# hive-proximum

<!-- hive-badges -->

[![Clojars Project](https://img.shields.io/clojars/v/io.github.hive-agi/hive-proximum.svg)](https://clojars.org/io.github.hive-agi/hive-proximum)
[![cljdoc](https://cljdoc.org/badge/io.github.hive-agi/hive-proximum)](https://cljdoc.org/d/io.github.hive-agi/hive-proximum/CURRENT)
[![release](https://github.com/hive-agi/hive-proximum/actions/workflows/release.yml/badge.svg)](https://github.com/hive-agi/hive-proximum/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

<!-- /hive-badges -->

**A bitemporal memory backend for the hive ecosystem, backed by
[Datahike](https://github.com/replikativ/datahike).** Nothing is overwritten and
nothing is hard-deleted, so a query can ask what the store believed at any past
moment — not merely what it holds now.

## Coordinates

```clojure
;; deps.edn
io.github.hive-agi/hive-proximum {:mvn/version "0.1.13"}
```

## The bitemporal design

- Every write is a new Datahike transaction — an immutable fact log.
- An update adds facts; the previous values stay in history.
- A delete sets `:entry/deleted? true` — a tombstone, never a hard delete.
- Temporal queries use `d/as-of` and `d/history` for time travel.

`hive-proximum.store` implements `IMemoryStore` and `IMemoryStoreTemporal`; the
temporal port is what separates this backend from the point-in-time ones.

## Vector storage

`hive-proximum.vec` adds an HNSW vector index behind `IVecStore`, a role-sized
port:

| Namespace | Role |
|---|---|
| `hive-proximum.vec.protocol` | `IVecStore` — the port |
| `hive-proximum.vec.store` | `ProximumVecStore` — adapter over the `org.replikativ/proximum` HNSW index |
| `hive-proximum.vec.config` | Typed config via `hive-di` `defconfig`, mirroring the Datalevin KG surface |

Because the config surface mirrors the other backends, swapping the vec slot is
a configuration change rather than a code change.

## Wiring

`hive-proximum.init` is the `IAddon` implementation — a `reify` over a
nil-railway pipeline, with **zero compile-time `hive-mcp` dependencies**;
everything host-side resolves through `requiring-resolve`.

`hive-proximum.lifecycle` registers an `IShutdownHook` at priority 215. It runs
*after* the pure client closers in the 210 band, because Proximum syncs its
konserve backing store to disk before releasing the Datahike connection and may
share a classloader path with other Datahike-backed stores that need to settle
first.

## License

MIT.
