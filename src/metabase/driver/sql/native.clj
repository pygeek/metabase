(ns metabase.driver.sql.native
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [korma.db :as kdb]
            [metabase.db :refer [sel]]
            [metabase.driver :as driver]
            [metabase.driver.interface :as i]
            (metabase.driver.sql [interface :as sql]
                                 [util :refer [db->korma-db]])
            [metabase.models.database :refer [Database]]
            [metabase.util :as u]))

(defn process-native
  "Process and run a native (raw SQL) QUERY."
  [{{sql :query} :native, database-id :database, :as query}]
  (try (let [database (sel :one :fields [Database :engine :details] :id database-id)
             db-conn  (-> database
                          db->korma-db
                          kdb/get-connection)
             driver   (driver/engine->driver (:engine database))]
         (jdbc/with-db-transaction [t-conn db-conn]
           (let [^java.sql.Connection jdbc-connection (:connection t-conn)]
             ;; Disable auto-commit for this transaction, that way shady queries are unable to modify the database
             (.setAutoCommit jdbc-connection false)
             (try
               ;; Set the timezone if applicable
               (when-let [timezone (driver/report-timezone)]
                 (when (and (seq timezone)
                            (contains? (i/features driver) :set-timezone))
                   (let [set-timezone-sql (sql/set-timezone-sql driver)]
                     (log/debug (u/format-color 'green "%s" set-timezone-sql))
                     (try (jdbc/db-do-prepared t-conn set-timezone-sql [timezone])
                          (catch Throwable e
                            (log/error (u/format-color 'red "Failed to set timezone: %s" (.getMessage e))))))))

               ;; Now run the query itself
               (log/debug (u/format-color 'green "%s" sql))
               (let [[columns & [first-row :as rows]] (jdbc/query t-conn sql, :as-arrays? true)]
                 {:rows    rows
                  :columns columns
                  :cols    (for [[column first-value] (zipmap columns first-row)]
                             {:name      column
                              :base_type (driver/class->base-type (type first-value))})})

               ;; Rollback any changes made during this transaction just to be extra-double-sure JDBC doesn't try to commit them automatically for us
               (finally (.rollback jdbc-connection))))))
       (catch java.sql.SQLException e
         (let [^String message (or (->> (.getMessage e)     ; error message comes back like 'Column "ZID" not found; SQL statement: ... [error-code]' sometimes
                                        (re-find #"^(.*);") ; the user already knows the SQL, and error code is meaningless
                                        second)             ; so just return the part of the exception that is relevant
                                   (.getMessage e))]
           (throw (Exception. message))))))
