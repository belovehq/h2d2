(ns h2d2.main
  (:require [clojure.core.matrix.dataset :as ds]
            [clojure.java.io :refer [file]]
            [clojure.string :as s]
            [h2d2 [db :as db] [tcp :as tcp]]))


;---------------------------------
; status messages

(defn db-status-message
  "Returns an information message about the database."
  []
  (if-let [n (db/table-count)]
    (str "The database contains " n " table(s).")
    "The database is closed."))


(defn tcp-status-message []
  "Returns an information message about the status of the TCP server."
  (let [{:keys [status port]} (tcp/status)]
    (case status
      :stopped (str "The TCP server is stopped and will restart on port " port ".")
      :started (str "The TCP server is running on port " port ".")
      "There is no TCP server yet.")))


(defn status-message []
  "Returns an information message about both database and TCP server."
  (str (db-status-message) " " (tcp-status-message)))


(def UhOh! "Oops, I don't understand this...")


;---------------------------------
; H2D2 multimethod

(defmulti
  h2d2
  "H2D2's method.
  If the first argument is a keyword, then the call is dispatched based on the keyword.
  Otherwise the call is dispatched based on the class of the first argument.

  No argument:
  Returns the status of db and server.

  The first argument is a keyword:
  :tcp :start      starts the tcp server on default port, or restarts it; returns status msg.
  :tcp :stop       stops the tcp server; returns status msg.
  :port 12345      start or restart the tcp server on port 12345; returns status msg.
  :wipe            empty the database; returns status msg.
  :close           close the database and stop the tcp server; returns status msg.
  :db              returns the database's spec.
  :list            returns the list of table names.
  :wipe            empty the database.
  :drop :mytable   remove mytable from the database.
  :top 10 :mytable returns the top 10 rows of mytable.
  :all :mytable    returns the whole content of mytable.

  The first argument is a String:
  If the string is the path of an existing file or URL then imports the file/URL as CSV using H2's
  built-in CSV import mechanism. Optional parameters are:
  name: the name of the table to create in the database. If absent, will try to use the name of the CSV file or else
  allocate a default name DATASETxx.
  columns: an array of column names or keys. If absent, the first row of the CSV file will be used as column definitions.
  If present, the whole CSV file will be read as data.
  Put the other way: this parameter must be provided if the first line of the CSV is a data line, and it must be left blank
  if the first line of the CSV file is a column header.
  separator: field separator: a character e.g. \\tab or a string e.g. \" \"
  comment: character that marks a line as a comment e.g. \\#
  port: a specific port on which to start or restart the TCP server

 The first argument is a core.matrix dataset:
 Imports the dataset into the database. Optional parameters are:
 name: the name of the table to create in the database. If absent, will try to use the name of the CSV file or else
 allocate a default name DATASETxx.
 columns: an array of column names or keys. If absent, the column names in the dataset will be used (and col numbers
 1, 2, 3... will be converted into C1, C2, C3...)
 type: A type definition as per the clojure.java.jdbc i.e. :int, :double... If not provided, all values in the dataset
 will be converted to VARCHAR.  If provided, then ALL values will be converted to the same type.
 port: a specific port on which to start or restart the TCP server

 The first argument is anything else:
 Attempts to convert the first argument into a core.matrix dataset, and then imports the dataset as above. Optional
 arguments are the same as for datasets: name, columns, type, port.
"

  (fn [& args] (let [x (first args)]
                 (if (keyword? x) x (class x)))))


(defmethod h2d2 nil [& args]
  (status-message))


(defmethod h2d2 :close [& args]
  (do
    (tcp/stop!)
    (db/close)
    (if (db/table-count)
      (str "The database will be closed when remaining connections are closed. " (tcp-status-message))
      (status-message))))



(defmethod h2d2 :all [& args]
  (apply db/select-all (rest args)))


(defmethod h2d2 :db [& args]
  db/db)


(defmethod h2d2 :drop [& args]
  (let [k (second args)
        out (db/drop-table! k)]
    (cond
      (nil? out) "This table doesn't exist!"
      out (str "Table " (s/upper-case (name k)) " was dropped.")
      :else ("A problem seems to have occurred. Table " (s/upper-case (name k)) " couldn't be dropped."))))


(defmethod h2d2 :list [& args]
  (db/tables))


(defmethod h2d2 :port [& args]
  (let [port (second args)]
    (if (tcp/port? port)
      (do
        (tcp/start! port)
        (status-message))
      UhOh!)))


(defmethod h2d2 :tcp [& args]
  (let [arg (second args)]
    (cond (= arg :start) (do
                           (tcp/start! nil)
                           (status-message))
          (= arg :stop) (do
                          (tcp/stop!)
                          (status-message))
          :else UhOh!
          )))


(defmethod h2d2 :top [& args]
  (apply db/select-top (rest args)))


(defmethod h2d2 :wipe [& args]
  (do
    (db/wipe!)
    (status-message)))


(defmethod h2d2 java.lang.String [& args]
  (let [[file-name & {:keys [table columns separator comment charset port]}] args
        is-url (try (not (nil? (java.net.URL. file-name))) (catch Exception e false))
        is-file (.exists (file file-name))]
    (if (not (or is-file is-url))
      "This is not a valid file or URL."
      (let [suggested-name (cond table table
                                 is-file (.getName (file file-name))
                                 :else nil)
            [table-name row-count] (db/csv! file-name suggested-name columns separator comment charset)]
        (do
          (tcp/start! port)
          (if row-count
            (str "Table " table-name " created with " row-count " rows. " (status-message))
            (str "A problem seems to have occurred; no table was created. " (status-message))))))))


(defmethod h2d2 :default [& args]
  (try
    (let [[data & {:keys [table columns type port]}] args
          [table-name row-count] (db/data! data table columns type)]
      (do
        (tcp/start! port)
        (if row-count
          (str "Table " table-name " created with " row-count " rows." (status-message))
          (str "A problem seems to have occurred; no table was created. " (status-message)))))
    (catch Exception e
      (if (s/starts-with? (.getMessage e) "Don't know how to create ")
        UhOh!
        (throw e)))))


;---------------------------------
; non-blocking version of h2d2

(defn h2d2go [& args]
  (do
    (future
      (println
        (try (apply h2d2 args)
             (catch Exception e (str e)))))
    nil))

