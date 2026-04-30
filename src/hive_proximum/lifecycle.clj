(ns hive-proximum.lifecycle
  "IShutdownHook for the Proximum Datahike-backed temporal store.
   Priority 215 — runs after pure client closers (qdrant/chroma at 210)
   because Proximum syncs its konserve backing store to disk before
   releasing the Datahike connection, and may share a classloader path
   with other datahike-backed stores that need to settle first.

   Zero compile-time coupling to hive-mcp shutdown internals beyond the
   IShutdownHook protocol and the registry — matches the runtime-
   resolve pattern used in `hive-proximum.init` for IAddon bits.

   Sync+close sequence:
     1. (hive-proximum.state/get-handle)  — snapshot backend handle
     2. Release Datahike connection via the store's protocol
        `disconnect!` (flushes konserve under the hood).
     3. (hive-proximum.state/clear-handle!) — atom bookkeeping.

   Each step is independently guarded; a failure in one does not skip
   the next. The outer `try` catches Throwable so the orchestrator's
   priority walk continues on error."
  ;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
  ;;
  ;; SPDX-License-Identifier: AGPL-3.0-or-later
  (:require [hive-mcp.protocols.lifecycle :as lifecycle]
            [hive-mcp.system.registry :as reg]
            [hive-proximum.state :as state]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Helpers — runtime protocol resolution (same style as init.clj)
;; =============================================================================

(defn- try-resolve [sym]
  (try (requiring-resolve sym) (catch Throwable _ nil)))

(defn- sync-and-release!
  "Best-effort: resolve the memory registry's disconnect! (which drives
   Datahike's `release`, syncing the konserve store) and invoke it on
   the registered :proximum store. Returns :ok on success, :skipped
   when the registry or store is not available."
  []
  (let [get-store   (try-resolve 'hive-mcp.protocols.memory/get-store)
        disconnect! (try-resolve 'hive-mcp.protocols.memory/disconnect!)]
    (if (and get-store disconnect!)
      (if-let [store (get-store :proximum)]
        (do (disconnect! store) :ok)
        :skipped)
      :skipped)))

;; =============================================================================
;; ProximumShutdown — priority 215 (client-to-store boundary)
;; =============================================================================

(defrecord ProximumShutdown []
  lifecycle/IShutdownHook
  (shutdown-priority [_] 215)
  (shutdown-name     [_] "proximum/sync-and-close")
  (shutdown!         [_ _ctx]
    (try
      (let [had-handle? (state/initialized?)
            release-result (sync-and-release!)]
        (try (state/clear-handle!) (catch Throwable t
                                     (log/warn t "proximum state clear failed")))
        (case release-result
          :ok      (log/info "proximum sync+close complete")
          :skipped (if had-handle?
                     (log/info "proximum sync+close: store not in memory registry, handle cleared")
                     (log/debug "proximum sync+close: not initialized — no-op"))))
      (catch Throwable t
        (log/error t "proximum sync+close failed")))))

;; =============================================================================
;; Registration
;; =============================================================================

(defn install! []
  (reg/register-shutdown! (->ProximumShutdown)))

;; Auto-register on ns load. `hive-proximum.init/init-as-addon!` or
;; hive-mcp `system/layer1` will require this ns; the defonce ensures
;; registration happens exactly once per classloader.
(defonce ^:private -registered?
  (do (install!) true))
