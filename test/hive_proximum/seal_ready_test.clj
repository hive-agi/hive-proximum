(ns hive-proximum.seal-ready-test
  "Seal readiness of ProximumStore: the :embed-text capability and
   IMemoryStoreScan, checked with hive-spi's own memory conformance cases
   and with stripping and scan tests beyond them. In-memory Datahike only."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [hive-proximum.store :as store]
            [hive-spi.memory.conformance :as conformance]
            [hive-spi.memory.decorate :as decorate]
            [hive-spi.memory.ports :as ports]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(defn- make-store
  "A fresh, connected, in-memory ProximumStore."
  []
  (let [s (store/create-store {:backend :memory})]
    (ports/connect! s {})
    s))

(defn- at-rest
  "Every datom the store's db holds, history included, as one string: what is
   in the fact log, whatever spelling put it there."
  [s]
  (pr-str (map (juxt :e :a :v) (d/datoms (d/history (d/db (store/ensure-conn! s))) :eavt))))

;; =============================================================================
;; hive-spi conformance cases
;; =============================================================================

(deftest conformance-embed-text
  (is (= :ran (conformance/run-case make-store {} :embed-text))))

(deftest conformance-scan
  (is (= :ran (conformance/run-case make-store {} :scan))))

;; =============================================================================
;; Declarations
;; =============================================================================

(deftest declares-seal-capabilities
  (let [s (make-store)]
    (testing "connected and disconnected status both declare :embed-text"
      (is (decorate/embed-text-capable? (ports/store-status s)))
      (ports/disconnect! s)
      (is (decorate/embed-text-capable? (ports/store-status s))))
    (is (satisfies? ports/IMemoryStoreScan s))))

;; =============================================================================
;; :embed-text is never persisted
;; =============================================================================

(deftest strip-transient-every-spelling-and-depth
  (is (= {:a 1 :c [{:d 2}] :s #{{:e 3}}}
         (store/strip-transient {:a 1 :embed-text "x" "embed-text" "y" 'embed-text "z"
                                 :n/embed-text "w"
                                 :c [{:d 2 :embed-text "v"}]
                                 :s #{{:e 3 :embed-text "u"}}})))
  (let [v [1.0 2.0 3.0]]
    (is (identical? v (store/strip-transient v)) "a flat vector is not rebuilt")))

(deftest add-never-persists-embed-text
  (let [s  (make-store)
        id (:id (ports/add-entry! s {:type :note
                                     :content {:body "visible" :embed-text "nested-secret"}
                                     :tags ["t"]
                                     :embed-text "top-secret"
                                     "embed-text" "string-secret"
                                     :x/embed-text "ns-secret"}))]
    (is (string? id))
    (doseq [secret ["top-secret" "string-secret" "ns-secret" "nested-secret" "embed-text"]]
      (is (not (str/includes? (at-rest s) secret)) secret))
    (is (= "visible" (get-in (ports/get-entry s id) [:content :body])))))

(deftest update-never-persists-embed-text
  (let [s  (make-store)
        id (:id (ports/add-entry! s {:type :note :content "cipher-0" :tags ["t"]}))]
    (ports/update-entry! s id {:embed-text "update-secret"
                               :content {:body "v2" :embed-text "nested-update-secret"}})
    (is (not (str/includes? (at-rest s) "update-secret")))
    (is (= "v2" (get-in (ports/get-entry s id) [:content :body])))
    (testing "an update carrying only :embed-text leaves the entry as it was"
      (ports/update-entry! s id {:embed-text "only-this"})
      (is (= "v2" (get-in (ports/get-entry s id) [:content :body])))
      (is (not (str/includes? (at-rest s) "only-this"))))))

;; =============================================================================
;; IMemoryStoreScan beyond the conformance case
;; =============================================================================

(deftest scan-skips-tombstones-and-honours-expiry
  (let [s       (make-store)
        live    (mapv #(:id (ports/add-entry! s {:type :note :content (str "live " %)})) (range 3))
        gone    (:id (ports/add-entry! s {:type :note :content "gone"}))
        expired (:id (ports/add-entry! s {:type :note :content "old"
                                          :expires "2000-01-01T00:00:00Z"}))
        later   (:id (ports/add-entry! s {:type :note :content "later"
                                          :expires "2099-01-01T00:00:00Z"}))]
    (ports/delete-entry! s gone)
    (is (= (set (conj live later)) (set (ports/scan-ids s {}))))
    (is (= (set (conj live later expired)) (set (ports/scan-ids s {:include-expired? true}))))
    (is (= (set (conj live later)) (set (ports/scan-ids s nil))) "nil opts are the defaults")))

(deftest scan-is-complete-past-any-page
  (let [s   (make-store)
        ids (set (map #(:id (ports/add-entry! s {:type :note :content (str "e" %)})) (range 1500)))
        got (ports/scan-ids s {})]
    (is (= (count got) (count (distinct got))) "no id twice")
    (is (= ids (set got)))))

(deftest scan-of-an-unreadable-store-throws
  (let [s (store/create-store {:config {:store {:backend :no-such-backend :id (random-uuid)}
                                        :schema-flexibility :read}})]
    (is (thrown? Exception (ports/scan-ids s {})))))
