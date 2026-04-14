(ns hive-proximum.state
  "Atom-based state for Proximum bitemporal memory backend.

   Holds backend handle (Datahike connection + config) once initialized."
  (:require [taoensso.timbre :as log]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;;; =============================================================================
;;; Backend Handle
;;; =============================================================================

(defonce ^:private backend-handle
  (atom {:initialized? false
         :backend      nil
         :config       nil}))

(defn get-handle
  "Return current backend handle snapshot."
  []
  @backend-handle)

(defn initialized?
  "Check if backend has been initialized."
  []
  (:initialized? @backend-handle))

(defn set-handle!
  "Store backend handle after initialization.
   Returns the new handle."
  [backend config]
  (let [h {:initialized? true
            :backend      backend
            :config       config}]
    (reset! backend-handle h)
    (log/debug "[proximum-state] Backend handle set" {:backend (type backend)})
    h))

(defn clear-handle!
  "Clear backend handle on shutdown."
  []
  (reset! backend-handle {:initialized? false
                          :backend      nil
                          :config       nil})
  (log/debug "[proximum-state] Backend handle cleared")
  nil)
