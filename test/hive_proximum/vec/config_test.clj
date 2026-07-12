;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-proximum.vec.config-test
  "Verify ProximumVecConfig honors env > config.edn > default chain."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-di.resolve :as resolve]
            [hive-proximum.vec.config :as cfg]
            [hive-dsl.result :as r]))

(def file-with-services
  (constantly {:services {:proximum {:path     "/from/file/path"
                                     :dim      768
                                     :capacity 50000}}}))

(deftest db-path-env-wins
  (testing "HIVE_VEC_PROXIMUM_PATH env var wins over config.edn"
    (let [result (resolve/resolve-config cfg/ProximumVecConfig-fields {}
                   {:env-fn (fn [v]
                              (case v
                                "HIVE_VEC_PROXIMUM_PATH" "/from/env"
                                nil))
                    :file-fn file-with-services})]
      (is (r/ok? result))
      (is (= "/from/env" (:db-path (:ok result)))))))

(deftest db-path-from-config-edn-when-env-unset
  (testing ":services :proximum :path config.edn key honored"
    (let [result (resolve/resolve-config cfg/ProximumVecConfig-fields {}
                   {:env-fn (constantly nil)
                    :file-fn file-with-services})]
      (is (r/ok? result))
      (is (= "/from/file/path" (:db-path (:ok result)))))))

(deftest db-path-default-when-nothing-set
  (testing "XDG default applies only when env + config.edn both empty"
    (let [result (resolve/resolve-config cfg/ProximumVecConfig-fields {}
                   {:env-fn (constantly nil)
                    :file-fn (constantly nil)})]
      (is (r/ok? result))
      (is (= cfg/default-db-path (:db-path (:ok result)))))))

(deftest dim-and-capacity-from-config-edn
  (let [result (resolve/resolve-config cfg/ProximumVecConfig-fields {}
                 {:env-fn (constantly nil)
                  :file-fn file-with-services})]
    (is (r/ok? result))
    (is (= 768 (:dim (:ok result))))
    (is (= 50000 (:capacity (:ok result))))))

(deftest dim-and-capacity-defaults-when-empty
  (let [result (resolve/resolve-config cfg/ProximumVecConfig-fields {}
                 {:env-fn (constantly nil)
                  :file-fn (constantly nil)})]
    (is (r/ok? result))
    (is (= cfg/default-dim      (:dim (:ok result))))
    (is (= cfg/default-capacity (:capacity (:ok result))))))

(deftest override-trumps-all
  (testing "Caller-passed override wins over env, config.edn, defaults"
    (let [result (resolve/resolve-config cfg/ProximumVecConfig-fields
                   {:db-path "/from/override" :dim 1024 :capacity 999}
                   {:env-fn (constantly "/from/env")
                    :file-fn file-with-services})]
      (is (r/ok? result))
      (is (= "/from/override" (:db-path (:ok result))))
      (is (= 1024 (:dim (:ok result))))
      (is (= 999  (:capacity (:ok result)))))))
