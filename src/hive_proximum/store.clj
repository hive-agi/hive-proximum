(ns hive-proximum.store
  "Datahike-backed IMemoryStore + IMemoryStoreTemporal for hive-proximum.

   Bitemporal design:
   - Every write is a new Datahike transaction (immutable fact log)
   - Updates add new facts; old values preserved in history
   - Deletes set :entry/deleted? true (tombstone, never hard-delete)
   - Temporal queries use d/as-of and d/history for time-travel

   Protocol extension uses `extend` (function, not macro) to avoid
   compile-time dependency on hive-mcp.protocols.memory.

   Backend: Datahike 0.8.1667 (matches hive-mcp/hive-knowledge)"
  (:require [datahike.api :as d]
            [hive-dsl.result :refer [guard rescue]]
            [taoensso.timbre :as log]
            [clojure.data.json :as json])
  (:import [java.util Date UUID]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; =============================================================================
;; Resolution Helper
;; =============================================================================

(defn- try-resolve
  "Attempt to resolve a fully-qualified symbol. Returns var or nil."
  [sym]
  (guard Exception nil (requiring-resolve sym)))

;; =============================================================================
;; Datahike Configuration
;; =============================================================================

(defn make-datahike-config
  "Build Datahike config map. Supports :memory and :file backends."
  ([] (make-datahike-config {}))
  ([{:keys [backend path]
     :or   {backend :memory
            path    "data/proximum/datahike"}}]
   {:store  {:backend backend
             :path    path
             :id      (UUID/randomUUID)}
    :schema-flexibility :read
    :index  :datahike.index/persistent-set}))

;; =============================================================================
;; Schema
;; =============================================================================

(def entry-schema
  [{:db/ident       :entry/id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :entry/type
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :entry/content
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :entry/tags
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/many
    :db/index       true}
   {:db/ident       :entry/content-hash
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :entry/created-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :entry/updated-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :entry/deleted?
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one}
   {:db/ident       :entry/expires-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :entry/source
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}])

(defn- ensure-schema!
  "Transact entry schema if not already present."
  [conn]
  (let [db (d/db conn)
        has-schema? (try
                      (d/pull db [:db/ident] :entry/id)
                      true
                      (catch Exception _ false))]
    (when-not has-schema?
      (d/transact conn {:tx-data entry-schema})
      (log/info "[proximum] Schema transacted"))))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn- now ^Date []
  (Date.))

(defn- generate-id []
  (str (UUID/randomUUID)))

(defn- content->hash [content]
  (when content
    (let [s (if (string? content) content (pr-str content))
          md (java.security.MessageDigest/getInstance "SHA-256")
          bytes (.digest md (.getBytes ^String s "UTF-8"))]
      (apply str (map #(format "%02x" %) bytes)))))

(defn entry->tx-data
  "Convert entry map to Datahike transaction data.

   `:entry/content` is canonically JSON-encoded so `entity->map` on read
   can deserialize back to a Clojure map. Pre-encoded strings (e.g. JSON
   produced by `memory-kanban`) pass through untouched."
  [{:keys [id type content tags content-hash source expires-at]}]
  (let [entry-id (or id (generate-id))
        ts (now)
        encoded-content (when content
                          (if (string? content) content (json/write-str content)))]
    (cond-> {:entry/id         entry-id
             :entry/created-at ts
             :entry/updated-at ts
             :entry/deleted?   false}
      type            (assoc :entry/type (keyword type))
      encoded-content (assoc :entry/content encoded-content)
      tags            (assoc :entry/tags (if (set? tags) tags (set (map str tags))))
      content-hash    (assoc :entry/content-hash content-hash)
      (and content (not content-hash))
      (assoc :entry/content-hash (content->hash content))
      source          (assoc :entry/source (str source))
      expires-at      (assoc :entry/expires-at expires-at))))

(defn pull-entry
  "Pull an entry from db, return nil if deleted or not found."
  [db id]
  (let [result (try
                 (d/pull db '[*] [:entry/id id])
                 (catch Exception _ nil))]
    (when (and result (:entry/id result) (not (:entry/deleted? result)))
      result)))

(defn pull-entry-raw
  "Pull an entry from db, including deleted entries."
  [db id]
  (try
    (let [result (d/pull db '[*] [:entry/id id])]
      (when (:entry/id result) result))
    (catch Exception _ nil)))

(defn entity->map
  "Convert Datahike entity map to clean entry map.

   `:entry/content` is stored as a JSON-encoded string by `add-entry!` /
   `update-entry!` (see `entry->tx-data`). Parse it back to a Clojure map
   on read so callers (e.g. kanban transitions) see structured content
   without round-tripping through `json/read-str`. Non-JSON content
   (legacy plain strings) is returned as-is."
  [e]
  (when e
    (let [raw-content (:entry/content e)
          parsed-content (if (and (string? raw-content)
                                  (#{\{ \[} (first raw-content)))
                           (rescue raw-content
                                   (json/read-str raw-content :key-fn keyword))
                           raw-content)]
      (cond-> {:id           (:entry/id e)
               :type         (:entry/type e)
               :content      parsed-content
               :created-at   (:entry/created-at e)
               :updated-at   (:entry/updated-at e)}
        (:entry/tags e)         (assoc :tags (set (:entry/tags e)))
        (:entry/content-hash e) (assoc :content-hash (:entry/content-hash e))
        (:entry/source e)       (assoc :source (:entry/source e))
        (:entry/expires-at e)   (assoc :expires-at (:entry/expires-at e))
        (some? (:entry/deleted? e)) (assoc :deleted? (:entry/deleted? e))))))

(defn- apply-order-by
  "Sort `entries` by `order-by` tuple `[field direction]`. `field` is a
   keyword on the entry map (e.g. :created-at). `direction` is :asc or :desc.
   When `order-by` is nil, falls back to `:created-at` ascending — preserves
   the legacy behavior of the unconditional `(sort-by :created-at)` this
   replaced."
  [entries order-by]
  (let [[field direction] (or order-by [:created-at :asc])
        cmp (if (= direction :desc)
              #(compare %2 %1)
              compare)]
    (sort-by field cmp entries)))

;; =============================================================================
;; ProximumStore defrecord
;; =============================================================================

(defrecord ProximumStore [conn-atom cfg])

(defn ensure-conn!
  "Ensure Datahike connection is established. Returns conn."
  [{:keys [conn-atom cfg]}]
  (when (nil? @conn-atom)
    (when-not (d/database-exists? cfg)
      (d/create-database cfg))
    (let [conn (d/connect cfg)]
      (ensure-schema! conn)
      (reset! conn-atom conn)
      (log/info "[proximum] Connected to Datahike" {:backend (get-in cfg [:store :backend])})))
  @conn-atom)

;; =============================================================================
;; Protocol Extension via `extend` (runtime, no compile-time dep)
;; =============================================================================

(defonce ^:private -protocols-extended? (atom false))

(defn extend-store-protocols!
  "Extend ProximumStore with IMemoryStore + IMemoryStoreTemporal.
   Uses `extend` (function) — protocol resolution is fully runtime.
   Safe to call multiple times (idempotent via atom guard)."
  []
  (when (compare-and-set! -protocols-extended? false true)
    ;; IMemoryStore
    (when-let [proto-var (try-resolve 'hive-mcp.protocols.memory/IMemoryStore)]
      (extend ProximumStore
        @proto-var
        {:connect!
         (fn [this config]
           (let [merged-cfg (merge (:cfg this) config)]
             (when-not (d/database-exists? merged-cfg)
               (d/create-database merged-cfg))
             (let [conn (d/connect merged-cfg)]
               (ensure-schema! conn)
               (reset! (:conn-atom this) conn)
               (log/info "[proximum] Connected" {:config merged-cfg})
               {:success? true})))

         :disconnect!
         (fn [this]
           (when-let [conn @(:conn-atom this)]
             (d/release conn)
             (reset! (:conn-atom this) nil)
             (log/info "[proximum] Disconnected"))
           {:success? true})

         :connected?
         (fn [this]
           (some? @(:conn-atom this)))

         :health-check
         (fn [this]
           (if-let [conn @(:conn-atom this)]
             (try
               (d/db conn)
               {:healthy? true
                :store-type :proximum
                :backend (get-in (:cfg this) [:store :backend])}
               (catch Exception e
                 {:healthy? false
                  :reason (.getMessage e)}))
             {:healthy? false
              :reason "Not connected"}))

         :add-entry!
         (fn [this entry]
           (let [conn (ensure-conn! this)
                 tx-data (entry->tx-data entry)
                 _result (d/transact conn {:tx-data [tx-data]})]
             (log/debug "[proximum] Entry added" {:id (:entry/id tx-data)})
             {:success? true
              :id (:entry/id tx-data)}))

         :get-entry
         (fn [this id]
           (let [conn (ensure-conn! this)
                 db (d/db conn)]
             (entity->map (pull-entry db id))))

         :update-entry!
         (fn [this id updates]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 existing (pull-entry-raw db id)]
             (if-not existing
               {:error :not-found :id id}
               (let [tx-data (cond-> {:entry/id id
                                      :entry/updated-at (now)}
                               (:content updates)
                               (assoc :entry/content
                                      (if (string? (:content updates))
                                        (:content updates)
                                        (json/write-str (:content updates))))
                               (:content updates)
                               (assoc :entry/content-hash
                                      (content->hash (:content updates)))
                               (:type updates)
                               (assoc :entry/type (keyword (:type updates)))
                               (:tags updates)
                               (assoc :entry/tags (set (map str (:tags updates))))
                               (:source updates)
                               (assoc :entry/source (str (:source updates)))
                               (:expires-at updates)
                               (assoc :entry/expires-at (:expires-at updates)))]
                 (d/transact conn {:tx-data [tx-data]})
                 (log/debug "[proximum] Entry updated" {:id id})
                 {:success? true :id id}))))

         :delete-entry!
         (fn [this id]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 existing (pull-entry-raw db id)]
             (if-not existing
               {:error :not-found :id id}
               (do
                 (d/transact conn {:tx-data [{:entry/id id
                                              :entry/deleted? true
                                              :entry/updated-at (now)}]})
                 (log/debug "[proximum] Entry tombstoned" {:id id})
                 {:success? true :id id}))))

         :query-entries
         (fn [this opts]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 {:keys [type tags limit order-by]} opts
                 results (cond
                           (and type tags)
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :in $ ?type ?tag
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]
                                  [?e :entry/type ?type]
                                  [?e :entry/tags ?tag]]
                                db (keyword type) (str (first tags)))

                           type
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :in $ ?type
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]
                                  [?e :entry/type ?type]]
                                db (keyword type))

                           tags
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :in $ ?tag
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]
                                  [?e :entry/tags ?tag]]
                                db (str (first tags)))

                           :else
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]]
                                db))]
             (->> results
                  (map entity->map)
                  (#(apply-order-by % order-by))
                  (cond->> limit (take limit)))))

         :search-similar
         (fn [_this _query-text _opts]
           {:results []
            :note "Proximum is a temporal store — semantic search delegates to milvus"})

         :supports-semantic-search?
         (fn [_this] false)

         :cleanup-expired!
         (fn [this]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 expired (d/q '[:find [(pull ?e [:entry/id]) ...]
                                :in $ ?now
                                :where
                                [?e :entry/expires-at ?exp]
                                [(< ?exp ?now)]
                                [?e :entry/deleted? false]]
                              db (now))]
             (when (seq expired)
               (d/transact conn {:tx-data (mapv (fn [e]
                                                  {:entry/id (:entry/id e)
                                                   :entry/deleted? true
                                                   :entry/updated-at (now)})
                                                expired)}))
             {:cleaned (count expired)}))

         :entries-expiring-soon
         (fn [this days _opts]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 cutoff (Date. (+ (.getTime (now)) (* days 24 60 60 1000)))
                 results (d/q '[:find [(pull ?e [*]) ...]
                                :in $ ?now ?cutoff
                                :where
                                [?e :entry/expires-at ?exp]
                                [(<= ?now ?exp)]
                                [(< ?exp ?cutoff)]
                                [?e :entry/deleted? false]]
                              db (now) cutoff)]
             (map entity->map results)))

         :find-duplicate
         (fn [this type content-hash _opts]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 results (d/q '[:find [(pull ?e [*]) ...]
                                :in $ ?type ?hash
                                :where
                                [?e :entry/type ?type]
                                [?e :entry/content-hash ?hash]
                                [?e :entry/deleted? false]]
                              db (keyword type) content-hash)]
             (first (map entity->map results))))

         :store-status
         (fn [this]
           (if-let [conn @(:conn-atom this)]
             (let [db (d/db conn)
                   cnt (d/q '[:find (count ?e) .
                              :where [?e :entry/id _]]
                            db)]
               {:store-type :proximum
                :status     :connected
                :backend    (get-in (:cfg this) [:store :backend])
                :entry-count (or cnt 0)})
             {:store-type :proximum
              :status     :disconnected}))

         :reset-store!
         (fn [this]
           (when-let [conn @(:conn-atom this)]
             (d/release conn)
             (reset! (:conn-atom this) nil))
           (when (d/database-exists? (:cfg this))
             (d/delete-database (:cfg this)))
           (log/info "[proximum] Store reset")
           {:success? true})}))

    ;; IMemoryStoreTemporal
    (when-let [proto-var (try-resolve 'hive-mcp.protocols.memory/IMemoryStoreTemporal)]
      (extend ProximumStore
        @proto-var
        {:asof-entry
         (fn [this id timestamp]
           (let [conn (ensure-conn! this)
                 db (d/as-of (d/db conn) timestamp)]
             (entity->map (pull-entry db id))))

         :history-entry
         (fn [this id]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 history-db (d/history db)
                 txs (d/q '[:find ?tx ?inst
                            :in $ ?id
                            :where
                            [?e :entry/id ?id ?tx true]
                            [?tx :db/txInstant ?inst]]
                          history-db id)]
             (->> txs
                  (sort-by second)
                  (mapv (fn [[_tx inst]]
                          [inst (entity->map
                                 (pull-entry-raw (d/as-of db inst) id))])))))

         :asof-query
         (fn [this criteria timestamp]
           (let [conn (ensure-conn! this)
                 db (d/as-of (d/db conn) timestamp)
                 {:keys [type tags]} criteria
                 results (cond
                           type
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :in $ ?type
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]
                                  [?e :entry/type ?type]]
                                db (keyword type))

                           tags
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :in $ ?tag
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]
                                  [?e :entry/tags ?tag]]
                                db (str (first tags)))

                           :else
                           (d/q '[:find [(pull ?e [*]) ...]
                                  :where
                                  [?e :entry/id _]
                                  [?e :entry/deleted? false]]
                                db))]
             (map entity->map results)))

         :between-query
         (fn [this criteria t1 t2]
           (let [conn (ensure-conn! this)
                 db (d/db conn)
                 history-db (d/history db)
                 {:keys [type tags]} criteria
                 entry-ids (d/q '[:find [?id ...]
                                  :in $ ?t1 ?t2
                                  :where
                                  [?e :entry/id ?id ?tx true]
                                  [?tx :db/txInstant ?inst]
                                  [(<= ?t1 ?inst)]
                                  [(<= ?inst ?t2)]]
                                history-db t1 t2)
                 as-of-db (d/as-of db t2)]
             (->> entry-ids
                  (map #(entity->map (pull-entry-raw as-of-db %)))
                  (filter some?)
                  (filter (fn [e]
                            (and (not (:deleted? e))
                                 (or (nil? type) (= (keyword type) (:type e)))
                                 (or (nil? tags) (some (:tags e #{}) (map str tags))))))
                  (sort-by :created-at))))}))))

;; =============================================================================
;; Store Constructor
;; =============================================================================

(defn create-store
  "Create a ProximumStore instance. Does NOT connect immediately (lazy).
   Options:
     :backend  - :memory (default) or :file
     :path     - file path for :file backend (default: data/proximum/datahike)
     :config   - raw Datahike config map (overrides :backend/:path)"
  ([] (create-store {}))
  ([opts]
   (let [cfg (or (:config opts) (make-datahike-config opts))
         store (->ProximumStore (atom nil) cfg)]
     (extend-store-protocols!)
     store)))

;; =============================================================================
;; Legacy compat — create-stub-store now returns a real store
;; =============================================================================

(defn create-stub-store
  "Create a ProximumStore. Replaces the old stub.
   Uses in-memory Datahike backend by default."
  []
  (create-store {:backend :memory}))
