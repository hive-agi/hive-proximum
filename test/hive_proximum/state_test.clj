(ns hive-proximum.state-test
  "Unit tests for proximum state management."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-proximum.state :as state]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(use-fixtures :each
  (fn [f]
    (state/clear-handle!)
    (f)
    (state/clear-handle!)))

(deftest initial-state
  (testing "starts uninitialized"
    (is (false? (state/initialized?)))
    (is (nil? (:backend (state/get-handle))))))

(deftest set-and-clear-handle
  (testing "set-handle! marks initialized"
    (state/set-handle! :stub {:some "config"})
    (is (true? (state/initialized?)))
    (is (= :stub (:backend (state/get-handle))))
    (is (= {:some "config"} (:config (state/get-handle)))))

  (testing "clear-handle! resets"
    (state/clear-handle!)
    (is (false? (state/initialized?)))
    (is (nil? (:backend (state/get-handle))))))
