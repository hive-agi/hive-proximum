;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(ns hive-proximum.vec.store
  "ProximumVecStore — IVecStore adapter over the proximum HNSW index
   (org.replikativ/proximum, ref: clones-ref/proximum).

   Two design constraints drive the shape of this namespace:

   1. Late-bind via `requiring-resolve` so a consumer that omits the
      proximum jar still compiles. The dep IS declared in this
      project's deps.edn, but addon-style isolation keeps the slot
      system honest — failures surface as `:slot/factory-failed`
      upstream instead of class-load errors.

   2. The proximum API is *immutable* (every op returns a new index
      value). Consumers cache one handle per slot and share it across
      the swarm, so we wrap the immutable index in an atom and
      synchronise mutations under it. Concurrent writers serialise
      through the atom, concurrent readers see a consistent snapshot.

   Reopen flow:
     `open!` first attempts `proximum.core/load` against the existing
     Konserve filestore. On miss (first boot / empty dir) it falls
     back to `proximum.core/create-index`. This means a process
     restart with the same `:db-path` resumes the same vectors —
     no manual snapshot wiring required."
  (:require [clojure.java.io :as io]
            [hive-dsl.result :as r :refer [rescue]]
            [hive-proximum.vec.config :as cfg]
            [hive-proximum.vec.protocol :as pvec]
            [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Late-bound proximum API resolver — never throws if the jar is missing
;; -----------------------------------------------------------------------------

(defn- resolve-fn
  [sym]
  (rescue nil (requiring-resolve sym)))

(defn- proximum-create-index [] (resolve-fn 'proximum.core/create-index))
(defn- proximum-load          [] (resolve-fn 'proximum.core/load))
(defn- proximum-insert        [] (resolve-fn 'proximum.core/insert))
(defn- proximum-search        [] (resolve-fn 'proximum.core/search))
(defn- proximum-delete        [] (resolve-fn 'proximum.core/delete))
(defn- proximum-count-vectors [] (resolve-fn 'proximum.core/count-vectors))
(defn- proximum-sync          [] (resolve-fn 'proximum.core/sync!))
(defn- proximum-close         [] (resolve-fn 'proximum.core/close!))

;; -----------------------------------------------------------------------------
;; Helpers (pure)
;; -----------------------------------------------------------------------------

(defn- validate-db-path!
  [db-path]
  (when (or (nil? db-path) (and (string? db-path) (empty? db-path)))
    (throw (ex-info "Proximum :db-path cannot be nil or empty"
                    {:db-path db-path})))
  (let [parent (.getParentFile (io/file db-path))]
    (when (and parent (not (.exists parent)))
      (log/info "Creating Proximum parent directory" {:parent (str parent)})
      (.mkdirs parent)))
  db-path)

(defn- existing-store?
  "True iff `db-path` looks like a previously-initialised Konserve
   filestore (directory exists AND contains content)."
  [db-path]
  (let [dir (io/file db-path)]
    (and (.exists dir) (.isDirectory dir) (boolean (seq (.list dir))))))

(defn- coerce-floats
  ^floats [v]
  (cond
    (nil? v)                            (float-array 0)
    (instance? (Class/forName "[F") v)  v
    (sequential? v)                     (float-array v)
    :else (throw (ex-info "vector must be a sequential or float-array"
                          {:got (type v)}))))

(defn- store-id-for-path
  ^java.util.UUID [^String path]
  (java.util.UUID/nameUUIDFromBytes (.getBytes (str "hive-proximum:" path))))

(defn- store-config [db-path]
  {:backend :file :path db-path :id (store-id-for-path db-path)})

(defn- build-create-config
  [{:keys [db-path dim capacity ^java.util.UUID store-id mmap-dir]}]
  {:type         :hnsw
   :dim          (long dim)
   :capacity     (long capacity)
   :store-config {:backend :file :path db-path :id store-id}
   :mmap-dir     (or mmap-dir db-path)})

(defn- load-existing
  [db-path]
  (when-let [load-fn (proximum-load)]
    (rescue nil
            (load-fn (store-config db-path)
                     :branch :main :mmap-dir db-path))))

(defn- create-fresh
  [db-path dim capacity]
  (when-let [create-fn (proximum-create-index)]
    (let [cfg (build-create-config
                {:db-path  db-path
                 :dim      dim
                 :capacity capacity
                 :store-id (store-id-for-path db-path)
                 :mmap-dir db-path})]
      (log/info "Creating fresh Proximum HNSW index"
                {:path db-path :dim dim :capacity capacity})
      (rescue nil (create-fn cfg)))))

(defn- open-index!
  "Open or create the underlying proximum HNSW index. Reopens an
   existing on-disk store via `load`; otherwise creates a fresh one."
  [db-path dim capacity]
  (validate-db-path! db-path)
  (if (existing-store? db-path)
    (do (log/info "Reopening Proximum HNSW index from existing store"
                  {:path db-path})
        (or (load-existing db-path)
            (create-fresh db-path dim capacity)))
    (create-fresh db-path dim capacity)))

;; -----------------------------------------------------------------------------
;; ProximumVecStore record
;; -----------------------------------------------------------------------------

(defrecord ProximumVecStore [idx-atom db-path dim capacity]
  pvec/IVecStore

  (open! [_this]
    (when (nil? @idx-atom)
      (when-let [idx (open-index! db-path dim capacity)]
        (reset! idx-atom idx)))
    @idx-atom)

  (close! [_this]
    (when-let [idx @idx-atom]
      (log/info "Closing Proximum HNSW index" {:path db-path})
      ;; Flush durable state BEFORE releasing — without sync the
      ;; in-memory index is dropped and `load` on the next open returns
      ;; an empty branch.
      (when-let [sync-fn (proximum-sync)]
        (rescue nil (sync-fn idx)))
      (when-let [close-fn (proximum-close)]
        (rescue nil (close-fn idx)))
      (reset! idx-atom nil)))

  (upsert! [this id v]
    (pvec/upsert! this id v nil))

  (upsert! [this id v metadata]
    ;; proximum.core/insert signature is (insert idx vector id) or
    ;; (insert idx vector id metadata) — vector BEFORE id, see
    ;; clones-ref/proximum/src/proximum/api_impl.clj#L43.
    (pvec/open! this)
    (when-let [insert-fn (proximum-insert)]
      (let [fv (coerce-floats v)]
        (rescue nil
                (swap! idx-atom
                       (fn [idx]
                         (if metadata
                           (insert-fn idx fv id metadata)
                           (insert-fn idx fv id)))))))
    this)

  (search [this query k]
    (pvec/search this query k nil))

  (search [this query k opts]
    (pvec/open! this)
    (or (when-let [search-fn (proximum-search)]
          (let [q (coerce-floats query)]
            (rescue nil
                    (if opts
                      (search-fn @idx-atom q k opts)
                      (search-fn @idx-atom q k)))))
        ()))

  (delete! [this id]
    (pvec/open! this)
    (when-let [delete-fn (proximum-delete)]
      (rescue nil (swap! idx-atom delete-fn id)))
    this)

  (vec-count [this]
    (pvec/open! this)
    (or (when-let [count-fn (proximum-count-vectors)]
          (rescue 0 (count-fn @idx-atom)))
        0))

  (store-status [_this]
    {:backend  :proximum
     :path     db-path
     :dim      dim
     :capacity capacity
     :open?    (some? @idx-atom)
     :count    (or (when-let [count-fn (proximum-count-vectors)]
                     (when-let [idx @idx-atom]
                       (rescue 0 (count-fn idx))))
                   0)}))

;; -----------------------------------------------------------------------------
;; Public constructor
;; -----------------------------------------------------------------------------

(defn- resolve-typed-config []
  (let [result (cfg/resolve-ProximumVecConfig)]
    (if (r/ok? result)
      (:ok result)
      (do (log/warn "ProximumVecConfig resolution failed; using bare defaults"
                    {:errors (:errors result)})
          {:db-path  cfg/default-db-path
           :dim      cfg/default-dim
           :capacity cfg/default-capacity}))))

(defn create-store
  "Create a new Proximum-backed IVecStore.

   Resolution order:
     1. Explicit caller args (:db-path, :dim, :capacity)
     2. ProximumVecConfig (env > config.edn > XDG default)

   Construction is cheap and side-effect free; the index is opened
   lazily on first `pvec/open!` (or first `upsert!`/`search`/etc)."
  [& [{:keys [db-path dim capacity]}]]
  (rescue nil
          (let [resolved          (resolve-typed-config)
                resolved-path     (or db-path  (:db-path  resolved))
                resolved-dim      (or dim      (:dim      resolved))
                resolved-capacity (or capacity (:capacity resolved))]
            (log/info "Creating Proximum vec store"
                      {:path resolved-path :dim resolved-dim
                       :capacity resolved-capacity})
            (->ProximumVecStore (atom nil)
                                resolved-path
                                (long resolved-dim)
                                (long resolved-capacity)))))
