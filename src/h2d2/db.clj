(ns h2d2.db
  (:require [clojure.core.matrix.dataset :as ds]
            [clojure.java.jdbc :as j]
            [clojure.string :as s]))


(def db {:classname      "org.h2.Driver"
         :subprotocol    "h2:mem"
         :subname        "H2D2"
         :user           ""
         :password       ""
         :DB_CLOSE_DELAY "-1"})

;locking object used in functions suffixed with an exclamation mark i.e. insert! csv! drop-table! wipe!
(defonce db-lock (Object.))


(defn table-exists?
  "Returns whether a table exists in the database"
  ([k] (try (table-exists? k (assoc db :IFEXISTS "TRUE"))
            (catch org.h2.jdbc.JdbcSQLException e
              (if (s/includes? (.getMessage e) "[90013-")
                false
                (throw e)))))
  ([k conn]
   (->> (j/query conn [(str "SELECT COUNT(*) AS count "
                            "FROM INFORMATION_SCHEMA.TABLES "
                            "WHERE TABLE_SCHEMA = 'PUBLIC' "
                            "AND TABLE_NAME = ?;")
                       (s/upper-case (name k))])
        first
        :count
        (= 1))))


(defn select-top
  "Returns the top n rows from a table in the database."
  [n table]
  (if (and (int? n) (table-exists? table))
    (j/query db [(str "SELECT TOP " n " * FROM " (s/upper-case (name table)) ";")])))


(defn select-all
  "Returns the top n rows from a table in the database."
  [table]
  (if (table-exists? table)
    (j/query db [(str "SELECT * FROM " (s/upper-case (name table)) ";")])))


(defn table-count
  "Returns the number of tables in the database. Returns nil if the database is closed.
  Use this function to test whether the database is open or not."
  []
  (try
    (->> (j/query (assoc db :IFEXISTS "TRUE")
                  [(str "SELECT COUNT(*) AS count "
                        "FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA';")])
         first
         :count)
    (catch org.h2.jdbc.JdbcSQLException e
      (if (s/includes? (.getMessage e) "[90013-")
        nil
        (throw e)))))


(defn tables []
  "Returns the list of tables in the database"
  (try
    (->> (j/query (assoc db :IFEXISTS "TRUE") [(str "SELECT TABLE_NAME AS name "
                                                    "FROM INFORMATION_SCHEMA.TABLES "
                                                    "WHERE TABLE_SCHEMA = 'PUBLIC' "
                                                    "ORDER BY TABLE_NAME;")])
         (map (comp keyword :name)))
    (catch org.h2.jdbc.JdbcSQLException e
      (if (s/includes? (.getMessage e) "[90013-")
        '()
        (throw e)))))

(defn close
  "Instructs the database do close once the last connection to it is closed."
  []
  (j/execute! db ["SET DB_CLOSE_DELAY 0;"]))


(defn version
  "Returns the version of H2 in use."
  []
  (->> (j/query db ["SELECT H2VERSION() AS version;"])
       first
       :version))


(defn make-table-name
  "Converts a string or keyword into a valid table name. If no string or keyword is supplied, creates a new generic
  table name that is compatible with the tables already in the database."
  ([] (if-let [n (->> (tables)
                      (map (comp s/upper-case name))
                      (filter #(and (> (count %) 7)
                                    (s/starts-with? (name %) "DATASET")
                                    (re-matches #"\p{Digit}+" (subs (name %) 7))))
                      (map #(Integer/parseInt (subs (name %) 7)))
                      (sort)
                      last)]
        (str "DATASET" (inc n))
        "DATASET1"))
  ([s-or-k]
   (if (nil? s-or-k)
     (make-table-name)
     (let [remove-ext (fn [s] (if (or (s/ends-with? s ".CSV") (s/ends-with? s ".TXT"))
                                (subs s 0 (- (count s) 4))
                                s))
           new-name (-> s-or-k
                        (name)
                        (s/upper-case)
                        (remove-ext)
                        (s/replace #"[^(\p{IsAlphabetic}|\p{Digit})]" ""))]
       (if (= new-name "") (make-table-name) new-name)))))


(defn to-typed-columns [cols type]
  "Transforms a list of columns and a type into a vector of typed columns."
  (->> cols
       (map #(vector
               (if (number? %) (str "C" %) (s/upper-case (name %)))
               (if type type "VARCHAR(MAX)")))
       vec))


(defn import-data
  "Imports a data structure into a new table in any H2 database. Returns a vector with the name of the table
   and the number of inserted rows. The data will be converted to a core/matrix dataset first.
     db: the database spec or connection
     data: the clojure data to load into the db
           (any data that can be passed to function clojure.core.matrix.dataset/dataset)
     name: the name of the table to create in the database.
     columns: an array of column names/keys (should conform to the columns format in clojure.java.jdbc).
              If absent, the columns will be named as per the columns of the dataset.
     type: a single type key or string such as :int, :double, \"varchar(50)\" (as per clojure.java.jdbc)
           that will apply to all columns of the table. If absent, the columns will all be varchar(max)."
  [db data name & {:keys [columns type]}]
    (let [dataset (ds/dataset data)
          reduce-block (fn [db-conn row-count block]
                         (apply + row-count (j/insert-multi! db-conn name nil block)))
          cols (if columns columns (ds/column-names dataset))
          blocks (->> dataset
                      (map vec)
                      (partition-all 100)
                      (map vec))]
      (j/with-db-connection
        [conn db]
        (if (table-exists? name conn)
          (j/db-do-commands conn (j/drop-table-ddl name {:entities clojure.string/upper-case})))
        (j/db-do-commands conn (j/create-table-ddl name (to-typed-columns cols type)))
        (vector name
                (reduce (partial reduce-block conn) 0 blocks)))))


(defn to-csv-command
  "Create a csv import statement as per H2's syntax"
  [file-name table-name columns separator line-comment charset]
  {:pre [(string? file-name) (string? table-name)]}
  (let [sep (cond
              (= separator \tab) "' || CHAR(9) || '"
              (#{\space " "} separator) "\\ "
              (#{\\ "\\"} separator) "\\\\"
              separator separator
              :else ",")
        comt (if
               (#{\\ "\\"} line-comment) "\\\\"
                                         line-comment)
        cols (if columns
               (->> columns
                    (map (comp s/upper-case name))
                    (interpose sep)
                    (apply str)
                    (#(str ", '" % "'")))
               ", NULL")
        opts (if (or separator line-comment charset)
               (str ", '"
                    (if separator (str "fieldSeparator=" sep))
                    (if (and separator (or line-comment charset)) " ")
                    (if line-comment (str "lineComment=" comt))
                    (if (and charset (or separator line-comment)) " ")
                    (if charset (str "charset=" (s/upper-case (name charset))))
                    "'"))]
    (-> (str "CREATE TABLE " table-name " AS SELECT * FROM CSVREAD('" file-name \' cols opts ");")
        (s/replace " || ''" ""))))


(defn import-csv
  "Imports a file/URL as CSV into any H2 database, using H2's built-in CSV import mechanism.
  db: the database spec or connection
  file-name: path of an existing file or URL
  name: the name of the table to create in the database.
  columns: an array of column names/keys. If absent, the first row of the CSV file will be used as column definitions. if present, the whole CSV file will be read as data.
  separator: field separator: a character e.g. \\tab or a string e.g. \" \"
  comment: character that marks a line as a comment e.g. \\#"
  [db file-name table-name  & {:keys [columns separator comment charset]}]
    (let [csv-command (to-csv-command file-name table-name columns separator comment charset)]
      (j/with-db-connection
        [conn db]
        (if (table-exists? table-name conn)
          (j/db-do-commands conn (j/drop-table-ddl table-name {:entities clojure.string/upper-case})))
        (j/db-do-commands conn csv-command)
        (vector table-name
                (->> (j/query conn
                              [(str "SELECT ROW_COUNT_ESTIMATE AS count "
                                    "FROM INFORMATION_SCHEMA.TABLES "
                                    "WHERE TABLE_SCHEMA = 'PUBLIC' "
                                    "AND TABLE_NAME = ?;") table-name])
                     first
                     :count)))))


(defn data!
  "Wraps the insert function, for use with the h2d2 database.
  Synchronised, so will block until other table creation and deletion tasks have completed.
  The function standardises the table name/key supplied, and will allocate a default name DATASETxx
  if no table name/key is supplied."
  [data table columns type]
  (locking db-lock
    (let [table-name (make-table-name table)]
      (import-data db data table-name :columns columns :type type))))


(defn csv!
  "Wraps the csv function, for use with the h2d2 database.
  Synchronised, so will block until other table creation and deletion tasks have completed.
  The function standardises the table name/key supplied, and will allocate a default name DATASETxx
  if no table name/key is supplied."
  [file-name table columns separator comment charset]
  (locking db-lock
    (let [table-name (make-table-name table)]
      (import-csv db file-name table-name :columns columns :separator separator :comment comment :charset charset))))


(defn wipe!
  "Empty the database.
  Synchronised, so will block until other table creation and deletion tasks have completed."
  []
  (locking db-lock
    (j/db-do-commands db ["DROP ALL OBJECTS;"])))


(defn drop-table!
  "Drops a table from the database.
  Synchronised, so will block until other table creation and deletion tasks have completed."
  [n]
  (locking db-lock
    (if (table-exists? n)
      (do (j/db-do-commands db (j/drop-table-ddl n {:entities clojure.string/upper-case}))
          (not (table-exists? n))))))
