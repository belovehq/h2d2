(ns h2d2.test-csv
  (:require [clojure.test :refer :all]
            [h2d2.main :refer [h2d2]]
            [clojure.string :as s]
            [h2d2.db :as db]
            [h2d2.test-fixtures :as fixtures]))


(use-fixtures :each fixtures/with-initialisation)


(def csvs {:artists     "testresources/artist_data.csv"
           :cameras     "testresources/Traffic enforcement cameras noheader.csv"
           :random      "testresources/random.csv"
           :randomspace "testresources/random-space.csv"
           :randomtab   "testresources/random-tab.txt"
           :sacramento  "http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv"})


(deftest csv-via-db-functions
  (testing "Test csv import scenario using functions in the h2d2.db namespace."

    ; database status before the database is created: table list, table count, table exists
    (is (= '() (db/tables)))
    (is (nil? (db/table-count)))
    (is (not (db/table-exists? :mytable)))

    ; import UTF-8 file and check: row-count (3532), special dash in second row, table count and table list
    (is (= ["ARTISTDATA" 3532] (db/csv! (:artists csvs) "artist_data.csv" nil nil nil :utf-8)))
    (is (= "1852–1911" (:dates (second (db/select-top 2 :artistdata)))))
    (is (= 1 (db/table-count)))
    (is (= '(:ARTISTDATA) (db/tables)))

    ; import file without header
    (is (= ["TRAFFICCAMS" 67]
           (db/csv! (:cameras csvs) "trafficcams"
                    [:locationcode :location :contraventioncode :contravention :activesince :hours]
                    nil nil nil)))
    (is (= "31J" (:contraventioncode (last (db/select-top 100 :trafficcams)))))
    (is (= 2 (db/table-count)))
    (is (= '(:ARTISTDATA :TRAFFICCAMS) (db/tables)))

    ; import a file with header, comment marker and space separator; and without providing a table name
    (is (= ["DATASET1" 100] (db/csv! (:randomspace csvs) nil nil \space \# nil)))
    (is (= "4/12/2007" (:date (first (db/select-top 1 :dataset1)))))
    (is (= 3 (db/table-count)))
    (is (= '(:ARTISTDATA :DATASET1 :TRAFFICCAMS) (db/tables)))

    ; remove a table
    (is (db/drop-table! :trafficcams))
    (is (nil? (db/drop-table! :dummy)))
    (is (= 2 (db/table-count)))
    (is (= '(:ARTISTDATA :DATASET1) (db/tables)))

    ; import a file with header and tab separator
    (is (= ["DATASET2" 100] (db/csv! (:randomtab csvs) nil nil \tab \# nil)))
    (is (= "4/12/2007" (:date (first (db/select-top 1 :dataset2)))))
    (is (= 3 (db/table-count)))
    (is (= '(:ARTISTDATA :DATASET1 :DATASET2) (db/tables)))

    ; import an url
    (is (= (db/drop-table! :dataset1)))
    (is (= ["DATASET3" 985] (db/csv! (:sacramento csvs) nil nil nil nil :utf-8)))
    (is (= "-121.442979" (:longitude (last (db/select-top 10 :dataset3)))))
    (is (= 3 (db/table-count)))

    ; wipe the database
    (is (= '(0) (db/wipe!)))
    (is (= '() (db/tables)))
    (is (= 0 (db/table-count)))))


(deftest csv-via-h2d2
  (testing "Test csv import scenario using the h2d2 function."

    ; table list
    (is (= '() (h2d2 :list)))
    (is (= (h2d2) "The database is closed. There is no TCP server yet."))

    ; error message when providing a string that is not a valid file or url
    (is (= "This is not a valid file or URL." (h2d2 "not-a-file")))

    ; import UTF-8 file and check: row-count (3532), special dash in second row, table count and table list
    (is (s/starts-with?
          (h2d2 (:artists csvs) :charset :utf-8)
          "Table ARTISTDATA created with 3532 rows. The database contains 1 table(s)."))
    (is (= "1852–1911" (:dates (second (h2d2 :top 2 :artistdata)))))
    (is (= '(:ARTISTDATA) (h2d2 :list)))
    (is (s/includes? (h2d2) "The TCP server is running"))

    ; import file without header
    (is (s/starts-with?
          (h2d2 (:cameras csvs) :table :trafficcams
                :columns [:locationcode :location :contraventioncode :contravention :activesince :hours])
          "Table TRAFFICCAMS created with 67 rows. The database contains 2 table(s)."))
    (is (= "31J" (:contraventioncode (last (h2d2 :top 100 :trafficcams)))))
    (is (= (h2d2 :list) '(:ARTISTDATA :TRAFFICCAMS)))


    ; import a file with header, comment marker and space separator; and without providing a table name
    (is (s/starts-with?
          (h2d2 (:randomspace csvs) :table "dataset1" :separator \space :comment \#)
          "Table DATASET1 created with 100 rows. The database contains 3 table(s)."))
    (is (= "4/12/2007" (:date (first (h2d2 :top 1 :dataset1)))))
    (is (= '(:ARTISTDATA :DATASET1 :TRAFFICCAMS) (h2d2 :list)))

    ; remove a table
    (is (= "Table TRAFFICCAMS was dropped." (h2d2 :drop :trafficcams)))
    (is (= "This table doesn't exist!" (h2d2 :drop :dummy)))
    (is '(:ARTISTDATA :DATASET1) (= (h2d2 :list)))

    ; import a file with header and tab separator and change port
    (is (=  (h2d2 (:randomtab csvs) :separator \tab :comment \# :port 58769)
          "Table RANDOMTAB created with 100 rows. The database contains 3 table(s). The TCP server is running on port 58769."))
    (is (= "4/12/2007" (:date (first (h2d2 :top 1 :randomtab)))))
    (is (= '(:ARTISTDATA :DATASET1 :RANDOMTAB) (h2d2 :list)))

    ; import an url
    (is (s/starts-with?
          (h2d2 (:sacramento csvs) :charset :utf-8)
          "Table DATASET2 created with 985 rows. The database contains 4 table(s)."))
    (is (= "-121.442979" (:longitude (last (h2d2 :top 10 "dataset2")))))
    (is '(:ARTISTDATA :DATASET1 :DATASET2 :RANDOMTAB) (= (h2d2 :list)))

    ; wipe the database
    (is (s/starts-with?
          (h2d2 :wipe)
          "The database contains 0 table(s)."))
    (is (= '() (h2d2 :list)))))


(comment
  (run-tests)
  )