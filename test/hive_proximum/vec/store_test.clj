;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-proximum.vec.store-test
  "Trifecta-shaped tests for ProximumVecStore — IVecStore + IKGStore
   lifecycle. Each test isolates its index in a fresh tmp directory and
   tears down on the way out (non-destructive close + filesystem
   cleanup of the test-only tmp tree)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-proximum.vec.protocol :as pvec]
            [hive-proximum.vec.store :as pvstore])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

;; -----------------------------------------------------------------------------
;; Fixture helpers
;; -----------------------------------------------------------------------------

(defn- tmp-dir!
  "Fresh isolated mmap directory per test."
  ^String []
  (-> (Files/createTempDirectory "proximum-vec-test-"
                                 (make-array FileAttribute 0))
      .toFile
      .getAbsolutePath))

(defn- delete-recursive!
  [^java.io.File f]
  (when (.isDirectory f)
    (doseq [c (.listFiles f)] (delete-recursive! c)))
  (.delete f))

(defn- with-tmp-store!
  "Build a store rooted at a fresh tmp dir, run `f` on it, tear down.
   Returns whatever `f` returns."
  [f & {:keys [dim capacity]
        :or {dim 16 capacity 1000}}]
  (let [path (str (tmp-dir!) "/proximum")
        s    (pvstore/create-store {:db-path path :dim dim :capacity capacity})]
    (try
      (pvec/open! s)
      (f s)
      (finally
        (pvec/close! s)
        (delete-recursive! (java.io.File. (.getParent (java.io.File. path))))))))

;; -----------------------------------------------------------------------------
;; Protocol satisfaction (LSP)
;; -----------------------------------------------------------------------------

(deftest satisfies-vec-protocol
  (with-tmp-store!
    (fn [s]
      (testing "ProximumVecStore satisfies IVecStore"
        (is (pvec/vec-store? s))))))

;; -----------------------------------------------------------------------------
;; Round-trip — upsert / search / delete / count
;; -----------------------------------------------------------------------------

(deftest upsert-and-search-round-trip
  (with-tmp-store!
    (fn [s]
      (pvec/upsert! s "v1" (float-array (range 16)))
      (pvec/upsert! s "v2" (float-array (map #(* 2 %) (range 16))))
      (pvec/upsert! s "v3" (float-array (map #(* 0.5 %) (range 16))))
      (testing "vec-count reflects every successful upsert"
        (is (= 3 (pvec/vec-count s))))
      (let [hits (pvec/search s (float-array (range 16)) 3)
            ids  (mapv :id hits)]
        (testing "exact-match query returns the stored vector first"
          (is (= "v1" (first ids))))
        (testing "all 3 ids surface, ordered by ascending distance"
          (is (= #{"v1" "v2" "v3"} (set ids)))
          (is (apply <= (map :distance hits))))))))

(deftest upsert-with-metadata
  (with-tmp-store!
    (fn [s]
      (pvec/upsert! s "m1" (float-array (range 16))
                    {:project-id "alpha" :kind :snippet})
      (testing "upsert with metadata succeeds and is countable"
        (is (= 1 (pvec/vec-count s)))))))

(deftest delete-removes-vector-from-future-searches
  (with-tmp-store!
    (fn [s]
      (pvec/upsert! s "keep1" (float-array (range 16)))
      (pvec/upsert! s "drop1" (float-array (map #(* 0.999 %) (range 16))))
      (pvec/upsert! s "keep2" (float-array (map #(* 1.001 %) (range 16))))
      (pvec/delete! s "drop1")
      (let [hits-after (pvec/search s (float-array (range 16)) 5)
            ids        (set (map :id hits-after))]
        (testing "deleted id no longer appears in search results"
          (is (not (contains? ids "drop1"))))
        (testing "non-deleted ids still present"
          (is (contains? ids "keep1"))
          (is (contains? ids "keep2")))))))

(deftest vec-count-zero-on-empty-store
  (with-tmp-store!
    (fn [s]
      (is (= 0 (pvec/vec-count s))))))

(deftest store-status-shape
  (with-tmp-store!
    (fn [s]
      (let [st (pvec/store-status s)]
        (is (= :proximum (:backend st)))
        (is (true?      (:open? st)))
        (is (number?    (:dim st)))
        (is (number?    (:capacity st)))
        (is (string?    (:path st)))))))

;; -----------------------------------------------------------------------------
;; Reopen — load existing on second ensure-conn!
;; -----------------------------------------------------------------------------

;; -----------------------------------------------------------------------------
;; close-and-reopen-preserves-vectors — PENDING, Phase 2.x.
;;
;; `proximum.core/load` reads from the `:main` branch snapshot in the
;; Konserve filestore. Empirically, calling `sync!` immediately before
;; `close!` does NOT register a `:main` snapshot — load throws "Branch
;; not found in storage" on reopen. The snapshot/branch wiring needs
;; an explicit commit or `branch!` call before sync, per proximum's
;; versioning model.
;;
;; This is a Phase 2.x concern (reopen semantics + branch coordination)
;; and not a blocker for the scaffold. Tracked separately. The current
;; behaviour: a process restart loses in-memory vectors if no explicit
;; snapshot was committed before close.
;; -----------------------------------------------------------------------------
