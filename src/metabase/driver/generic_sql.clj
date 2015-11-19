(ns metabase.driver.generic-sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [korma.core :as k]
            [korma.sql.utils :as kutils]
            [metabase.driver :as driver]
            [metabase.driver.interface :as i]
            [metabase.driver.generic-sql.query-processor :as qp]
            (metabase.driver.sql [interface :as sql]
                                 [util :as sqlutil])
            [metabase.util :as u])
  (:import clojure.lang.Keyword
           java.util.Map))

(def ^:private ^:const field-values-lazy-seq-chunk-size
  "How many Field values should we fetch at a time for `field-values-lazy-seq`?"
  ;; Hopefully this is a good balance between
  ;; 1. Not doing too many DB calls
  ;; 2. Not running out of mem
  ;; 3. Not fetching too many results for things like mark-json-field! which will fail after the first result that isn't valid JSON
  500)

(def ISQLDriverDefaultsMixin
  "Default implementations of *OPTIONAL* `ISQLDriver` methods."
  {:current-datetime-fn (constantly (k/sqlfn* :NOW))
   :excluded-schemas    (constantly nil)
   :qp-clause->handler  qp/clause->handler
   :set-timezone-sql    (constantly nil)
   :stddev-fn           (constantly :STDDEV)})


;;; ## IDriver Implementations

(defn features [driver]
  (set (cond-> [:foreign-keys
                :standard-deviation-aggregations]
         (sql/set-timezone-sql driver) (conj :set-timezone))))

(defn- can-connect? [driver details-map]
  (let [connection (sql/connection-details->spec driver details-map)]
    (= 1 (-> (k/exec-raw connection "SELECT 1" :results)
             first
             vals
             first))))

(defn- process-query [_ query]
  (qp/process-and-run query))

(defn- sync-in-context [_ database f]
  (sqlutil/with-jdbc-metadata [_ database]
    (f)))

(defn- active-tables [driver database]
  (sqlutil/with-jdbc-metadata [^java.sql.DatabaseMetaData md database]
    (set (for [table (filter #(not (contains? (sql/excluded-schemas driver) (:table_schem %)))
                             (jdbc/result-set-seq (.getTables md nil nil nil (into-array String ["TABLE", "VIEW"]))))]
           {:name   (:table_name table)
            :schema (:table_schem table)}))))

(defn- active-column-names->type [driver table]
  (sqlutil/with-jdbc-metadata [^java.sql.DatabaseMetaData md @(:db table)]
    (into {} (for [{:keys [column_name type_name]} (jdbc/result-set-seq (.getColumns md nil (:schema table) (:name table) nil))]
               {column_name (or (sql/column->base-type driver (keyword type_name))
                                (do (log/warn (format "Don't know how to map column type '%s' to a Field base_type, falling back to :UnknownField." type_name))
                                    :UnknownField))}))))

(defn- table-pks [_ table]
  (sqlutil/with-jdbc-metadata [^java.sql.DatabaseMetaData md @(:db table)]
    (->> (.getPrimaryKeys md nil nil (:name table))
         jdbc/result-set-seq
         (map :column_name)
         set)))

(defn- field-values-lazy-seq [_ {:keys [qualified-name-components table], :as field}]
  (assert (and (map? field)
               (delay? qualified-name-components)
               (delay? table))
    (format "Field is missing required information:\n%s" (u/pprint-to-str 'red field)))
  (let [table           @table
        name-components (rest @qualified-name-components)
        ;; This function returns a chunked lazy seq that will fetch some range of results, e.g. 0 - 500, then concat that chunk of results
        ;; with a recursive call to (lazily) fetch the next chunk of results, until we run out of results or hit the limit.
        fetch-chunk     (fn -fetch-chunk [start step limit]
                          (lazy-seq
                           (let [results (->> (k/select (sqlutil/korma-entity table)
                                                        (k/fields (:name field))
                                                        (k/offset start)
                                                        (k/limit (+ start step)))
                                              (map (keyword (:name field)))
                                              (map (if (contains? #{:TextField :CharField} (:base_type field)) u/jdbc-clob->str
                                                       identity)))]
                             (concat results (when (and (seq results)
                                                        (< (+ start step) limit)
                                                        (= (count results) step))
                                               (-fetch-chunk (+ start step) step limit))))))]
    (fetch-chunk 0 field-values-lazy-seq-chunk-size
                 i/max-sync-lazy-seq-results)))

(defn- table-rows-seq [_ database table-name]
  (k/select (-> (k/create-entity table-name)
                (k/database (sqlutil/db->korma-db database)))))


(defn- table-fks [_ table]
  (sqlutil/with-jdbc-metadata [^java.sql.DatabaseMetaData md @(:db table)]
    (->> (.getImportedKeys md nil nil (:name table))
         jdbc/result-set-seq
         (map (fn [result]
                {:fk-column-name   (:fkcolumn_name result)
                 :dest-table-name  (:pktable_name result)
                 :dest-column-name (:pkcolumn_name result)}))
         set)))

(defn- field-avg-length [driver field]
  (or (some-> (sqlutil/korma-entity @(:table field))
              (k/select (k/aggregate (avg (k/sqlfn* (sql/string-length-fn driver)
                                                    (kutils/func "CAST(%s AS CHAR)"
                                                                 [(keyword (:name field))])))
                                     :len))
              first
              :len
              int)
      0))

(defn- field-percent-urls [_ field]
  (or (let [korma-table (sqlutil/korma-entity @(:table field))]
        (when-let [total-non-null-count (:count (first (k/select korma-table
                                                                 (k/aggregate (count (k/raw "*")) :count)
                                                                 (k/where {(keyword (:name field)) [not= nil]}))))]
          (when (> total-non-null-count 0)
            (when-let [url-count (:count (first (k/select korma-table
                                                          (k/aggregate (count (k/raw "*")) :count)
                                                          (k/where {(keyword (:name field)) [like "http%://_%.__%"]}))))]
              (float (/ url-count total-non-null-count))))))
      0.0))


(def IDriverSQLDefaultsMixin
  "Default Implementaions of `IDriver` methods for drivers that implement `ISQLDriver`."
  (merge driver/IDriverDefaultsMixin
         {:active-column-names->type active-column-names->type
          :active-tables             active-tables
          :can-connect?              can-connect?
          :features                  features
          :field-avg-length          field-avg-length
          :field-percent-urls        field-percent-urls
          :field-values-lazy-seq     field-values-lazy-seq
          :process-query             process-query
          :sync-in-context           sync-in-context
          :table-fks                 table-fks
          :table-pks                 table-pks
          :table-rows-seq            table-rows-seq}))
