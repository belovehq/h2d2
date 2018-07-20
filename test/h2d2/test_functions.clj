(ns h2d2.test-functions
  (:require [clojure.test :refer :all]
            [h2d2.main :as hd]
            [h2d2.db :as db]
            [h2d2.tcp :as tcp]))

(deftest tcp!port?
  (testing "Whether a value is a valid port number."
    (is (tcp/port? 1024))
    (is (tcp/port? 65535))
    (is (not (tcp/port? 1023)))
    (is (not (tcp/port? 65536)))
    (is (not (tcp/port? nil)))
    (is (not (tcp/port? "abc")))))

(deftest tcp!need-new-server?
  (testing "Whether a new TCP server should be instantiated."
    (is (tcp/need-new-server? nil nil))
    (is (tcp/need-new-server? nil 9093))
    (is (tcp/need-new-server? 9092 9093))
    (is (tcp/need-new-server? "abc" 9093))
    (is (tcp/need-new-server? "abc" nil))
    (is (not (tcp/need-new-server? 9092 9092)))
    (is (not (tcp/need-new-server? 9092 nil)))
    (is (not (tcp/need-new-server? nil 1023)))
    (is (not (tcp/need-new-server? nil "abc")))
    (is (not (tcp/need-new-server? 9092 "abc")))
    (is (not (tcp/need-new-server? "abc" "abc")))))

(deftest db!make-table-name
  (testing "Generating table names."
    (with-redefs [db/tables (fn [] '(:table1 :table2))]
      (is (= "ABC" (db/make-table-name "abc")))
      (is (= "ABC" (db/make-table-name :abc)))
      (is (= "ABC" (db/make-table-name "abc.csv")))
      (is (= "ABC" (db/make-table-name :abc.csv)))
      (is (= "ABC" (db/make-table-name "abc.txt")))
      (is (= "ABC" (db/make-table-name :abc.txt)))
      (is (= "ABCDOC" (db/make-table-name "abc.doc")))
      (is (= "ABCDOC" (db/make-table-name :abc.doc)))
      (is (= "ABC" (db/make-table-name "aB:c_")))
      (is (= "ABC" (db/make-table-name :aB:c!)))
      (is (= "DATASET1" (db/make-table-name)))
      (is (= "DATASET1" (db/make-table-name ".csv")))
      (is (= "DATASET1" (db/make-table-name ".txt")))
      (is (= "DOC" (db/make-table-name ".doc")))
      (is (= "DATASET1" (db/make-table-name "'!_?")))
      (is (= "DATASET1" (db/make-table-name :!_))))
    (with-redefs [db/tables (fn [] '(:table1 :table2 :dataset3))]
      (is (= "ABC" (db/make-table-name "abc")))
      (is (= "ABC" (db/make-table-name :abc)))
      (is (= "ABC" (db/make-table-name "abc.csv")))
      (is (= "ABC" (db/make-table-name :abc.csv)))
      (is (= "ABCDOC" (db/make-table-name "abc.doc")))
      (is (= "ABCDOC" (db/make-table-name :abc.doc)))
      (is (= "ABC" (db/make-table-name "aB:c_")))
      (is (= "ABC" (db/make-table-name :aB:c!)))
      (is (= "DATASET4" (db/make-table-name)))
      (is (= "DATASET4" (db/make-table-name ".csv")))
      (is (= "DATASET4" (db/make-table-name "'!_?")))
      (is (= "DATASET4" (db/make-table-name :!_)))
      )))


(deftest db!to-typed-columns
  (testing "Generating typed columns."
    (is (= [["COL_A" "VARCHAR(MAX)"] ["COL_B" "VARCHAR(MAX)"] ["COL_C" "VARCHAR(MAX)"]]
           (db/to-typed-columns '(:col_a :col_b :col_c) nil)))
    (is (= [["COL_A" "VARCHAR(MAX)"] ["COL_B" "VARCHAR(MAX)"] ["COL_C" "VARCHAR(MAX)"]]
           (db/to-typed-columns ["col_a" "col_b" "col_c"] nil)))
    (is (= [["C1" "VARCHAR(MAX)"] ["C2" "VARCHAR(MAX)"] ["C3" "VARCHAR(MAX)"]]
           (db/to-typed-columns '(1 2 3) nil)))
    (is (= [["C1" :int] ["C2" :int] ["C3" :int]]
           (db/to-typed-columns '(1 2 3) :int)))
    (is (= [["COL_A" :whatever] ["COL_B" :whatever] ["COL_C" :whatever]]
           (db/to-typed-columns '(:col_a :col_b :col_c) :whatever)))
    (is (= [["C1" "whatever"] ["C2" "whatever"] ["C3" "whatever"]]
           (db/to-typed-columns '(1 2 3) "whatever")))
    (is (= [[";'/" :whatever] ["{}!" :whatever]]
           (db/to-typed-columns '(";'/" "{}!") :whatever)))
    (is (= [] (db/to-typed-columns nil nil)))
    (is (= [] (db/to-typed-columns nil :int)))
    (is (thrown? NullPointerException (db/to-typed-columns [nil nil nil] :int)))))


(deftest db!to-csv-command
  (testing "Generating CSV import statement."
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', NULL);"
           (db/to-csv-command "myfile.csv" "mytable" nil nil nil nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'A,B,C');"
           (db/to-csv-command "myfile.csv" "mytable" [:a :b :c] nil nil nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'A,B,C', 'lineComment=#');"
           (db/to-csv-command "myfile.csv" "mytable" ["a" "b" "c"] nil \# nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'A;B;C', 'fieldSeparator=;');"
           (db/to-csv-command "myfile.csv" "mytable" [:a :b :c] \; nil nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', NULL, 'fieldSeparator=; lineComment=#');"
           (db/to-csv-command "myfile.csv" "mytable" nil \; "#" nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', NULL, 'fieldSeparator=' || CHAR(9));"
           (db/to-csv-command "myfile.csv" "mytable" nil \tab nil nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'C1\\ C2\\ C3', 'fieldSeparator=\\ ');"
           (db/to-csv-command "myfile.csv" "mytable" [:c1 :c2 :c3] \space nil nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'C1' || CHAR(9) || 'C2' || CHAR(9) || 'C3', 'fieldSeparator=' || CHAR(9) || ' lineComment=@');"
           (db/to-csv-command "myfile.csv" "mytable" [:c1 :c2 :c3] \tab \@ nil)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', NULL, 'charset=UTF-8');"
           (db/to-csv-command "myfile.csv" "mytable" nil nil nil "UTF-8")))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', NULL, 'lineComment=# charset=UTF-8');"
           (db/to-csv-command "myfile.csv" "mytable" nil nil \# :UTF-8)))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'C1,C2,C3', 'charset=UTF-8');"
           (db/to-csv-command "myfile.csv" "mytable" ["c1" "c2" "c3"] nil nil "UTF-8")))
    (is (= "CREATE TABLE mytable AS SELECT * FROM CSVREAD('myfile.csv', 'C1' || CHAR(9) || 'C2' || CHAR(9) || 'C3', 'fieldSeparator=' || CHAR(9) || ' lineComment=@ charset=UTF-8');"
           (db/to-csv-command "myfile.csv" "mytable" [:c1 :c2 :c3] \tab \@ :UTF-8)))
    (is (thrown? AssertionError (db/to-csv-command "myfile.csv" 123 nil nil nil nil)))
    (is (thrown? AssertionError (db/to-csv-command 123 "mytable" nil nil nil nil)))))

(deftest statuses
  (testing "Status messages"
    (with-redefs [db/table-count (fn [] 3)]
      (is (= "The database contains 3 table(s)." (hd/db-status-message)))
      (with-redefs [tcp/status (fn [] {:status :stopped :port 9092})]
        (is (= "The TCP server is stopped and will restart on port 9092." (hd/tcp-status-message)))
        (is (= "The database contains 3 table(s). The TCP server is stopped and will restart on port 9092." (hd/status-message))))
      (with-redefs [tcp/status (fn [] {:status :started :port 9092})]
        (is (= "The TCP server is running on port 9092." (hd/tcp-status-message)))
        (is (= "The database contains 3 table(s). The TCP server is running on port 9092." (hd/status-message))))
      (with-redefs [tcp/status (fn [] {})]
        (is (= "There is no TCP server yet." (hd/tcp-status-message)))
        (is (= "The database contains 3 table(s). There is no TCP server yet." (hd/status-message)))))
    (with-redefs [db/table-count (fn [] nil)
                  tcp/status (fn [] {})]
      (is (= "The database is closed." (hd/db-status-message)))
      (is (= "The database is closed. There is no TCP server yet." (hd/status-message))))
    (is (string? hd/UhOh!))))

(deftest h2d2
  (testing "h2d2 pass-through methods."
    (is (= (hd/h2d2) (hd/status-message)))
    (is (= (hd/h2d2 :db) db/db))
    (with-redefs [db/tables (fn [] #{:whatever})]
      (is (= (hd/h2d2 :list) (db/tables) #{:whatever})))
    (with-redefs [db/select-top (fn [& args] args)]
      (is (= (hd/h2d2 :top 1 :table) (db/select-top 1 :table) [1 :table])))))


(comment
  (run-tests)
  )