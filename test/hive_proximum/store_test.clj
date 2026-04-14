(ns hive-proximum.store-test
  "Tests for Datahike-backed ProximumStore.

   Tests Datahike CRUD directly (no hive-mcp protocol dependency).
   Protocol-level integration tests require hive-mcp on classpath."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [hive-proximum.store :as store])
  (:import [java.util Date]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn- make-test-store
  "Create an in-memory ProximumStore for testing."
  []
  (store/create-store {:backend :memory}))

(defn- connect-store!
  "Ensure the store is connected and return it."
  [store]
  (store/ensure-conn! store)
  store)

(defn- cleanup-store!
  "Release connection and delete database."
  [store]
  (when-let [conn @(:conn-atom store)]
    (d/release conn)
    (reset! (:conn-atom store) nil))
  (when (d/database-exists? (:cfg store))
    (d/delete-database (:cfg store))))

(defn- transact-entry!
  "Low-level entry transact for testing (bypasses protocol)."
  [store entry-map]
  (let [conn @(:conn-atom store)
        tx-data (store/entry->tx-data entry-map)]
    (d/transact conn {:tx-data [tx-data]})
    (:entry/id tx-data)))

(defn- get-entry-direct
  "Low-level entry read for testing (bypasses protocol)."
  [store id]
  (let [db (d/db @(:conn-atom store))]
    (store/entity->map (store/pull-entry db id))))

(defn- get-entry-raw
  "Low-level entry read including tombstoned entries."
  [store id]
  (let [db (d/db @(:conn-atom store))]
    (store/entity->map (store/pull-entry-raw db id))))

;; =============================================================================
;; Store Creation Tests
;; =============================================================================

(deftest create-store-test
  (testing "creates ProximumStore with atom and config"
    (let [store (make-test-store)]
      (is (instance? hive_proximum.store.ProximumStore store))
      (is (some? (:conn-atom store)))
      (is (nil? @(:conn-atom store))) ;; lazy — not connected yet
      (is (= :memory (get-in (:cfg store) [:store :backend]))))))

(deftest datahike-config-test
  (testing "default config is in-memory"
    (let [cfg (store/make-datahike-config)]
      (is (= :memory (get-in cfg [:store :backend])))
      (is (= :read (:schema-flexibility cfg)))))

  (testing "file backend config"
    (let [cfg (store/make-datahike-config {:backend :file :path "/tmp/test"})]
      (is (= :file (get-in cfg [:store :backend])))
      (is (= "/tmp/test" (get-in cfg [:store :path]))))))

;; =============================================================================
;; CRUD Tests (Direct Datahike, no protocol)
;; =============================================================================

(deftest round-trip-crud-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "add and get entry"
        (let [id (transact-entry! store {:id "test-1"
                                          :type :decision
                                          :content "Use Datahike for bitemporal"
                                          :tags #{"architecture" "backend"}})]
          (is (= "test-1" id))
          (let [entry (get-entry-direct store "test-1")]
            (is (= "test-1" (:id entry)))
            (is (= :decision (:type entry)))
            (is (= "Use Datahike for bitemporal" (:content entry)))
            (is (contains? (:tags entry) "architecture"))
            (is (some? (:created-at entry)))
            (is (false? (:deleted? entry))))))

      (testing "update entry (new fact, no overwrite)"
        (let [conn @(:conn-atom store)
              _ (d/transact conn {:tx-data [{:entry/id "test-1"
                                              :entry/content "Use Datahike v2"
                                              :entry/updated-at (Date.)}]})
              entry (get-entry-direct store "test-1")]
          (is (= "Use Datahike v2" (:content entry)))
          ;; History should have 2 versions
          (let [history-db (d/history (d/db conn))
                tx-count (d/q '[:find (count ?tx) .
                                :in $ ?id
                                :where
                                [?e :entry/id ?id]
                                [?e :entry/content _ ?tx true]]
                              history-db "test-1")]
            (is (= 2 tx-count) "Should have 2 content versions in history"))))

      (testing "delete entry (tombstone)"
        (let [conn @(:conn-atom store)
              _ (d/transact conn {:tx-data [{:entry/id "test-1"
                                              :entry/deleted? true
                                              :entry/updated-at (Date.)}]})
              entry (get-entry-direct store "test-1")]
          (is (nil? entry) "Deleted entry should return nil via pull-entry")
          ;; But raw pull should still find it
          (let [raw (get-entry-raw store "test-1")]
            (is (some? raw))
            (is (true? (:deleted? raw))))))

      (finally
        (cleanup-store! store)))))

;; =============================================================================
;; Temporal Query Tests
;; =============================================================================

(deftest temporal-asof-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "as-of returns value at point in time"
        (let [conn @(:conn-atom store)
              ;; Add entry
              _ (transact-entry! store {:id "temporal-1"
                                         :type :plan
                                         :content "Version 1"})
              t1 (Date.)
              _ (Thread/sleep 50)
              ;; Update entry
              _ (d/transact conn {:tx-data [{:entry/id "temporal-1"
                                              :entry/content "Version 2"
                                              :entry/updated-at (Date.)}]})

              ;; Current value should be Version 2
              current (get-entry-direct store "temporal-1")
              _ (is (= "Version 2" (:content current)))

              ;; as-of t1 should return Version 1
              db-t1 (d/as-of (d/db conn) t1)
              asof-entry (store/entity->map (store/pull-entry db-t1 "temporal-1"))]
          (is (= "Version 1" (:content asof-entry))
              "as-of t1 should return original value")))

      (finally
        (cleanup-store! store)))))

(deftest temporal-history-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "history returns all versions"
        (let [conn @(:conn-atom store)
              _ (transact-entry! store {:id "hist-1"
                                         :type :session-summary
                                         :content "Summary v1"})
              _ (Thread/sleep 20)
              _ (d/transact conn {:tx-data [{:entry/id "hist-1"
                                              :entry/content "Summary v2"
                                              :entry/updated-at (Date.)}]})
              _ (Thread/sleep 20)
              _ (d/transact conn {:tx-data [{:entry/id "hist-1"
                                              :entry/content "Summary v3"
                                              :entry/updated-at (Date.)}]})
              history-db (d/history (d/db conn))
              content-versions (d/q '[:find [?content ...]
                                      :in $ ?id
                                      :where
                                      [?e :entry/id ?id]
                                      [?e :entry/content ?content]]
                                    history-db "hist-1")]
          (is (= 3 (count content-versions))
              "Should have 3 content versions")
          (is (= #{"Summary v1" "Summary v2" "Summary v3"}
                 (set content-versions)))))

      (finally
        (cleanup-store! store)))))

(deftest tombstone-preserves-history-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "tombstone preserves pre-deletion value via as-of"
        (let [conn @(:conn-atom store)
              _ (transact-entry! store {:id "tomb-1"
                                         :type :kanban
                                         :content "Important task"})
              t-before (Date.)
              _ (Thread/sleep 50)
              ;; Tombstone
              _ (d/transact conn {:tx-data [{:entry/id "tomb-1"
                                              :entry/deleted? true
                                              :entry/updated-at (Date.)}]})
              ;; Current: nil (deleted)
              current (get-entry-direct store "tomb-1")
              _ (is (nil? current) "Deleted entry returns nil")

              ;; as-of pre-deletion: should return value
              db-before (d/as-of (d/db conn) t-before)
              pre-delete (store/entity->map (store/pull-entry db-before "tomb-1"))]
          (is (some? pre-delete) "Pre-deletion as-of should return entry")
          (is (= "Important task" (:content pre-delete)))))

      (finally
        (cleanup-store! store)))))

;; =============================================================================
;; Query Tests
;; =============================================================================

(deftest query-by-type-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "query entries by type"
        (transact-entry! store {:id "q1" :type :decision :content "Decision 1"})
        (transact-entry! store {:id "q2" :type :decision :content "Decision 2"})
        (transact-entry! store {:id "q3" :type :plan :content "Plan 1"})
        (let [db (d/db @(:conn-atom store))
              decisions (d/q '[:find [(pull ?e [*]) ...]
                               :in $ ?type
                               :where
                               [?e :entry/type ?type]
                               [?e :entry/deleted? false]]
                             db :decision)]
          (is (= 2 (count decisions)))
          (is (every? #(= :decision (:entry/type %)) decisions))))

      (finally
        (cleanup-store! store)))))

(deftest query-by-tags-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "query entries by tag"
        (transact-entry! store {:id "t1" :type :decision :content "D1" :tags #{"urgent" "arch"}})
        (transact-entry! store {:id "t2" :type :plan :content "P1" :tags #{"urgent"}})
        (transact-entry! store {:id "t3" :type :plan :content "P2" :tags #{"low-pri"}})
        (let [db (d/db @(:conn-atom store))
              urgent (d/q '[:find [(pull ?e [*]) ...]
                            :in $ ?tag
                            :where
                            [?e :entry/tags ?tag]
                            [?e :entry/deleted? false]]
                          db "urgent")]
          (is (= 2 (count urgent)))))

      (finally
        (cleanup-store! store)))))

;; =============================================================================
;; Between Query Test (Datahike temporal)
;; =============================================================================

(deftest between-query-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "find entries modified between t1 and t2"
        (let [conn @(:conn-atom store)
              _ (transact-entry! store {:id "bw-1" :type :decision :content "Early"})
              t1 (Date.)
              _ (Thread/sleep 50)
              _ (transact-entry! store {:id "bw-2" :type :plan :content "Middle"})
              t-mid (Date.)
              _ (Thread/sleep 50)
              _ (transact-entry! store {:id "bw-3" :type :kanban :content "Late"})

              ;; Query entries transacted between t1 and t-mid
              history-db (d/history (d/db conn))
              ids-between (d/q '[:find [?id ...]
                                 :in $ ?t1 ?t2
                                 :where
                                 [?e :entry/id ?id ?tx true]
                                 [?tx :db/txInstant ?inst]
                                 [(<= ?t1 ?inst)]
                                 [(<= ?inst ?t2)]]
                               history-db t1 t-mid)]
          (is (= #{"bw-2"} (set ids-between))
              "Only bw-2 was transacted between t1 and t2")))

      (finally
        (cleanup-store! store)))))

;; =============================================================================
;; Content Hash Test
;; =============================================================================

(deftest content-hash-test
  (testing "auto-generates content hash"
    (let [store (connect-store! (make-test-store))]
      (try
        (transact-entry! store {:id "hash-1" :type :decision :content "Test content"})
        (let [entry (get-entry-raw store "hash-1")]
          (is (some? (:content-hash entry)) "Should auto-generate content hash")
          (is (= 64 (count (:content-hash entry))) "SHA-256 hash is 64 hex chars"))
        (finally
          (cleanup-store! store))))))

;; =============================================================================
;; Stress Test
;; =============================================================================

(deftest stress-test
  (let [store (connect-store! (make-test-store))]
    (try
      (testing "100 entries, 10 updates each"
        (let [conn @(:conn-atom store)
              start (System/currentTimeMillis)]
          ;; Insert 100 entries
          (doseq [i (range 100)]
            (transact-entry! store {:id (str "stress-" i)
                                     :type :session-summary
                                     :content (str "Content " i)
                                     :tags #{(str "batch-" (mod i 5))}}))
          ;; Update each 10 times
          (doseq [i (range 100)
                  j (range 10)]
            (d/transact conn {:tx-data [{:entry/id (str "stress-" i)
                                          :entry/content (str "Content " i " v" (inc j))
                                          :entry/updated-at (Date.)}]}))
          (let [elapsed (- (System/currentTimeMillis) start)
                db (d/db conn)
                total (d/q '[:find (count ?e) .
                             :where [?e :entry/id _]]
                           db)]
            (is (= 100 total) "Should have 100 entries")
            ;; Verify history depth for a sample entry
            (let [history-db (d/history db)
                  versions (d/q '[:find (count ?tx) .
                                  :in $ ?id
                                  :where
                                  [?e :entry/id ?id]
                                  [?e :entry/content _ ?tx true]]
                                history-db "stress-0")]
              (is (= 11 versions) "stress-0 should have 11 content versions (1 + 10 updates)"))
            (println (str "  Stress test: 100 entries x 10 updates = "
                          elapsed "ms")))))

      (finally
        (cleanup-store! store)))))
