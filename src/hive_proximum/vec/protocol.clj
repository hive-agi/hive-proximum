;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(ns hive-proximum.vec.protocol
  "IVecStore — role-sized port for vector index storage backends.

   Lives in hive-proximum (the provider) so consumers can late-bind via
   `requiring-resolve` without dragging hive-proximum's full classpath
   into their compile-time graph. The protocol is the contract; the
   record is the deliverable; nothing else crosses the seam.

   Lifecycle methods (`open!`, `close!`) live ON this protocol — NOT
   on a sibling lifecycle protocol — to avoid the JVM-level method-name
   collision that `defrecord` raises when one record satisfies two
   protocols that share a method name.

   Naming note: `vec-count` (not `count`) is used because `defrecord`
   inherits `clojure.lang.Counted/count` and a same-named protocol
   method would collide on the Java method signature."
  (:refer-clojure :exclude [count]))

;; -----------------------------------------------------------------------------
;; IVecStore — minimum viable vector-store surface
;; -----------------------------------------------------------------------------

(defprotocol IVecStore
  "Role-sized vector store port. Implementations must be safe to call
   from multiple threads — slot registries cache one handle per slot
   and share it across the swarm."

  (open! [this]
    "Initialise / reopen the underlying index. Idempotent. Returns the
     live handle (impl-defined) for chaining; callers SHOULD treat the
     return as opaque.")

  (close! [this]
    "Release native resources / file handles. Idempotent.
     NON-DESTRUCTIVE — must NOT delete on-disk state per AXIOM
     'Never NUKE Data — Destruction Requires Explicit, Loud, Guarded
     Consent'.")

  (upsert! [this id ^floats vector]
           [this id ^floats vector metadata]
    "Insert (or replace) `vector` under external key `id`. Returns
     `this` so callers may thread successive upserts. `metadata` is an
     optional map attached to the vector for filtered search /
     downstream pulls.")

  (search [this ^floats query k]
          [this ^floats query k opts]
    "k-NN search: return a seq of `{:id :distance ...}` maps ordered by
     ascending distance. `opts` may include impl-specific tuning (e.g.
     `:ef` for HNSW beam width). Empty seq when index is empty.")

  (delete! [this id]
    "Remove the vector keyed by external `id`. Returns `this`. No-op
     when the id is absent (must NOT throw).")

  (vec-count [this]
    "Return the number of live vectors in the index. 0 for an empty /
     freshly opened store. See namespace docstring for the rename
     rationale.")

  (store-status [this]
    "Return a diagnostic snapshot — `{:backend :proximum :path \"...\"
     :count N :open? true|false}` (impl-defined keys). Used by health
     surfaces; MUST be cheap (no full scans)."))

;; -----------------------------------------------------------------------------
;; Predicate
;; -----------------------------------------------------------------------------

(defn vec-store?
  "True when `x` satisfies IVecStore. Safe on `nil`."
  [x]
  (and (some? x) (satisfies? IVecStore x)))
