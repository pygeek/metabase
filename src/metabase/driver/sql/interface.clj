(ns metabase.driver.sql.interface)

(defprotocol ISQLDriver
  "Methods SQL-based drivers should implement in order to use `IDriverSQLDefaultsMixin`.
   Methods marked *OPTIONAL* have default implementations in `ISQLDriverDefaultsMixin`."
  (column->base-type ^clojure.lang.Keyword [this, ^Keyword column-type]
    "Given a native DB column type, return the corresponding `Field` `base-type`.")
  (string-length-fn ^clojure.lang.Keyword [this]
    "Keyword name of the SQL function that should be used to get the length of a string, e.g. `:LENGTH`.")
  (stddev-fn ^clojure.lang.Keyword [this]
    "*OPTIONAL*. Keyword name of the SQL function that should be used to get the length of a string. Defaults to `:STDDEV`.")
  (current-datetime-fn [this]
    "*OPTIONAL*. Korma form that should be used to get the current `DATETIME` (or equivalent). Defaults to `(k/sqlfn* :NOW)`.")
  (connection-details->spec [this, ^Map details-map]
    "Given a `Database` DETAILS-MAP, return a JDBC connection spec.")
  (unix-timestamp->timestamp [this, ^Keyword seconds-or-milliseconds, field-or-value]
    "Return a korma form appropriate for converting a Unix timestamp integer field or value to an proper SQL `Timestamp`.
     SECONDS-OR-MILLISECONDS refers to the resolution of the int in question and with be either `:seconds` or `:milliseconds`.")
  (set-timezone-sql ^String [this]
    "*OPTIONAL*. This should be a prepared JDBC SQL statement string to be used to set the timezone for the current transaction.

       \"SET @@session.timezone = ?;\"")
  (date [this, ^Keyword unit, field-or-value]
    "Return a korma form for truncating a date or timestamp field or value to a given resolution, or extracting a
     date component.")
  (date-interval [this, ^Keyword unit, ^Number amount]
    "Return a korma form for a date relative to NOW(), e.g. on that would produce SQL like `(NOW() + INTERVAL '1 month')`.")
  (excluded-schemas ^clojure.util.Set [this]
    "*OPTIONAL*. Set of string names of schemas to skip syncing tables from.")
  (qp-clause->handler ^java.util.Map [this]
    "*OPTIONAL*. A map of query processor clause keywords to functions of the form `(fn [korma-query query-map])` that are used apply them.
     By default, its value is `metabase.driver.generic-sql.query-processor/clause->handler`. These functions are exposed in this way so drivers
     can override default clause application behavior where appropriate -- for example, SQL Server needs to override the function used to apply the
     `:limit` clause, since T-SQL uses `TOP` rather than `LIMIT`."))
