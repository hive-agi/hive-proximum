(ns hive-proximum.init
  "IAddon implementation for hive-proximum — temporal/bitemporal memory backend.

   Follows the hive-tmux exemplar: reify + nil-railway pipeline.
   Zero compile-time hive-mcp dependencies — all resolved via requiring-resolve.

   Registers a Datahike-backed IMemoryStore implementing bitemporal
   storage (IMemoryStoreTemporal). Backend: Datahike 0.8.1667.

   Usage:
     ;; Via addon system (auto-discovered from META-INF manifest):
     (init-as-addon!)"
  (:require [hive-proximum.state :as state]
            [hive-proximum.store :as store]
            [hive-dsl.result :refer [guard]]
            [taoensso.timbre :as log]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; =============================================================================
;; Resolution Helpers
;; =============================================================================

(defn- try-resolve
  "Attempt to resolve a fully-qualified symbol. Returns var or nil.
   Uses guard instead of raw try-catch."
  [sym]
  (guard Exception nil (requiring-resolve sym)))

;; =============================================================================
;; Stub IMemoryStore (delegated to hive-proximum.store)
;; =============================================================================

;; =============================================================================
;; IAddon Implementation
;; =============================================================================

(defonce ^:private addon-instance (atom nil))

(defn- make-addon
  "Create an IAddon reify for hive-proximum.
   Returns nil if protocol is not on classpath."
  []
  (when (try-resolve 'hive-mcp.addons.protocol/IAddon)
    (let [state (atom {:initialized? false})]
      (reify
        hive-mcp.addons.protocol/IAddon

        (addon-id [_] "hive.proximum")

        (addon-type [_] :native)

        (capabilities [_] #{:memory-store :health-reporting})

        (initialize! [_ _config]
          (if (:initialized? @state)
            {:success? true :already-initialized? true}
            ;; Step 1: Preflight — verify IMemoryStore protocol available
            (let [stub-store (store/create-stub-store)]
              (if-not stub-store
                (do
                  (log/error "hive-proximum preflight failed: IMemoryStore protocol not on classpath")
                  {:success? false
                   :errors ["IMemoryStore protocol not on classpath — is hive-mcp loaded?"]})
                ;; Step 2: Register Datahike store under :proximum key
                (if-let [register-fn (try-resolve 'hive-mcp.protocols.memory/register-store!)]
                  (do
                    (register-fn :proximum stub-store)
                    (state/set-handle! :datahike (:cfg stub-store))
                    (swap! state assoc :initialized? true :store stub-store)
                    (log/info "hive-proximum addon initialized — Datahike IMemoryStore registered under :proximum")
                    {:success? true
                     :errors []
                     :metadata {:store-key :proximum
                                :store-type :datahike
                                :backend (get-in (:cfg stub-store) [:store :backend])}})
                  (do
                    (log/error "register-store! not available on classpath")
                    {:success? false
                     :errors ["Memory store registry not available — is hive-mcp loaded?"]}))))))

        (shutdown! [_]
          (when (:initialized? @state)
            ;; Unregister memory store
            (when-let [unreg-fn (try-resolve 'hive-mcp.protocols.memory/unregister-store!)]
              (unreg-fn :proximum))
            (state/clear-handle!)
            (reset! state {:initialized? false})
            (log/info "hive-proximum addon shut down"))
          nil)

        (tools [_] [])

        (schema-extensions [_] {})

        (excluded-tools [_] #{})

        (health [_]
          (if (:initialized? @state)
            (let [store (:store @state)
                  store-healthy? (try
                                  (when (and store (.connected? store))
                                    (:healthy? (.health-check store)))
                                  (catch Exception _ false))]
              {:status (if store-healthy? :ok :degraded)
               :details {:store-key :proximum
                         :store-type :datahike
                         :connected? (boolean store-healthy?)}})
            {:status :down
             :details {:reason "not initialized"}}))))))

;; =============================================================================
;; Dep Registry + Nil-Railway Pipeline
;; =============================================================================

(defonce ^:private dep-registry
  (atom {:register! 'hive-mcp.addons.core/register-addon!
         :init!     'hive-mcp.addons.core/init-addon!
         :addon-id  'hive-mcp.addons.protocol/addon-id}))

(defn- resolve-deps
  "Resolve all symbols in registry. Returns ctx map or nil."
  [registry]
  (reduce-kv
   (fn [ctx k sym]
     (if-let [resolved (try-resolve sym)]
       (assoc ctx k resolved)
       (do (log/debug "Dep resolution failed:" k "->" sym)
           (reduced nil))))
   {}
   registry))

(defn- step-resolve-deps [ctx]
  (when-let [deps (resolve-deps @dep-registry)]
    (merge ctx deps)))

(defn- step-register [{:keys [addon register!] :as ctx}]
  (let [result (register! addon)]
    (when (:success? result)
      (assoc ctx :reg-result result))))

(defn- step-init [{:keys [addon addon-id init!] :as ctx}]
  (let [result (init! (addon-id addon))]
    (when (:success? result)
      (assoc ctx :init-result result))))

(defn- step-store-instance [{:keys [addon] :as ctx}]
  (reset! addon-instance addon)
  ctx)

(defn- run-addon-pipeline!
  "Nil-railway: resolve-deps -> register -> init -> store"
  [initial-ctx]
  (some-> initial-ctx
          step-resolve-deps
          step-register
          step-init
          step-store-instance))

;; =============================================================================
;; Public API
;; =============================================================================

(defn init-as-addon!
  "Register hive-proximum as an IAddon. Returns registration result."
  []
  (if-let [_result (some-> (make-addon)
                           (as-> addon (run-addon-pipeline! {:addon addon})))]
    (do
      (log/info "hive-proximum registered as IAddon")
      {:registered ["proximum"] :total 1})
    (do
      (log/debug "IAddon unavailable — hive-proximum addon registration failed")
      {:registered [] :total 0})))

(defn get-addon-instance
  "Return the current IAddon instance, or nil."
  []
  @addon-instance)
