(ns h2d2.test-dataset
  (:require [clojure.test :refer :all]
            [h2d2.main :refer [h2d2 UhOh!]]
            [clojure.string :as s]
            [h2d2.db :as db]
            [h2d2.test-fixtures :as fixtures]
            [clojure.core.matrix.dataset :as ds]
            [clojure.core.matrix :as m]))


; TODO: Update tests of the db/data! function with all kinds of structures, incl vecs of maps and datasets
; TODO: Add tests of db/upload-data and db/upload-csv functions

(use-fixtures :each fixtures/with-initialisation fixtures/with-vectorz)

(def edns {:vecs "testresources/random-vecs.edn"
           :maps "testresources/random-maps.edn"})

(deftest dataset-via-db-functions
  (testing "Imports vecs-of-vecs using functions in the h2d2.db namespace."

    ; vec of vec from file, to varchar values
    (is (= ["DATASET1" 100] (db/data! (read-string (slurp (:vecs edns))) nil [:id :code :number :string :date] nil)))
    (is (= "8937" (:number (last (db/select-top 100 "dataset1")))))

    ; vec of vec of numbers, to varchar values
    (is (= ["SOMENUMBERS" 11] (db/data! (take 11 (partition 5 10 (range)))
                                          :somenumbers (range 5) nil)))
    (is (= "100" (:c0 (last (db/select-all :somenumbers)))))

    ; vec of vec of numbers, to int
    (is (= ["DATASET2" 11] (db/data! (take 11 (partition 5 10 (range)))
                                       nil (range 5) :int)))
    (is (= 100 (:c0 (last (db/select-all "dataset2")))))

    ; vec of vec of numbers, to double
    (is (= ["DATASET3" 11] (db/data! (take 11 (partition 5 10 (range)))
                                       nil (range 5) "double")))
    (is (= 54.0 (:c4 (last (db/select-top 6 "dataset3")))))

    ;nil values
    (is (thrown? clojure.lang.ExceptionInfo (db/data! nil nil nil nil)))

    ; exception when attempting an invalid type conversion
    (is (thrown? org.h2.jdbc.JdbcBatchUpdateException (db/data! [["abc"]] nil [:str] :int)))))



(deftest dataset-via-h2d2
  (testing "Imports datasets, vecs, maps, matrices using the h2d2 function."

    ; import a dataset made of mixed types with user-defined column names.
    (is (s/starts-with?
          (h2d2 (ds/dataset [:id :code :number :string :date] (read-string (slurp (:vecs edns)))))
          "Table DATASET1 created with 100 rows.The database contains 1 table(s)."))
    (is (= "8937" (:number (last (h2d2 :top 100 "dataset1")))))

    ; import a dataset made of ints and with default column names. Cast the dataset to double when importing.
    (is (s/starts-with?
          (h2d2 (ds/dataset (take 11 (partition 5 10 (range))))
                :table :doubles
                :type :double)
          "Table DOUBLES created with 11 rows.The database contains 2 table(s)."))
    (is (= 54.0 (:c4 (last (h2d2 :top 6 :doubles)))))

    ; import a dataset of ints, with default column names. Provide new column names at import time.
    (is (s/starts-with?
          (h2d2 (ds/dataset (take 11 (partition 5 10 (range))))
                :columns ["a" :b "c" :d "e"])
          "Table DATASET2 created with 11 rows.The database contains 3 table(s)."))
    (is (= "100" (:a (last (h2d2 :all "dataset2")))))

    ; import a vec of vecs, without specifying column names.
    (is (s/starts-with?
          (h2d2 (read-string (slurp (:vecs edns))))
          "Table DATASET3 created with 100 rows.The database contains 4 table(s)."))
    (is (= "8937" (:c2 (last (h2d2 :all :dataset3)))))

    ; import a vec of maps, without specifying column names.
    (is (s/starts-with?
          (h2d2 (read-string (slurp (:maps edns))))
          "Table DATASET4 created with 100 rows.The database contains 5 table(s)."))
    (is (= "8937" (:number (last (h2d2 :all :dataset4)))))

    ; import a vec of maps, specifying new column names, and change port
    (is (= (h2d2 (read-string (slurp (:maps edns))) :columns [:a :b :c :d :e] :port 58967)
           "Table DATASET5 created with 100 rows.The database contains 6 table(s). The TCP server is running on port 58967."))
    (is (= "8937" (:c (last (h2d2 :all :dataset5)))))

    ; import a matrix, with a custom name, and cast to int
    (is (s/starts-with?
          (h2d2 (m/diagonal-matrix (range 20)) :table :matrix :type :int)
          "Table MATRIX created with 20 rows.The database contains 7 table(s)."))
    (is (= 19 (:c19 (last (h2d2 :all :matrix)))))

    ; error message when the argument cannot be converted into a core.matrix dataset
    (is (= UhOh! (h2d2 1)))
    (is (= UhOh! (h2d2 [1])))))

(comment
  (run-tests)
  )