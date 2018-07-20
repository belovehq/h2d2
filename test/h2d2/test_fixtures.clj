(ns h2d2.test-fixtures
  (:require [clojure.test :refer :all]
            [h2d2.tcp :as tcp]
            [h2d2.db :as db]
            [clojure.java.io :as io]
            [clojure.core.matrix :as m]))

(defn with-initialisation [f]
  (try
    (db/close)
    (tcp/stop!)
    (reset! tcp/h2server nil)
    (f)
    (finally
      (db/close)
      (tcp/stop!)
      (reset! tcp/h2server nil))))

(defn with-vectorz [f]
  (let [impl clojure.core.matrix.implementations/*matrix-implementation*]
  (try
    (m/set-current-implementation :vectorz)
    (f)
    (finally
      (m/set-current-implementation impl)))))


