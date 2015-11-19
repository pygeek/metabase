(ns metabase.driver.interface
  (:import (clojure.lang IFn Keyword)
           java.util.Map
           metabase.models.database.DatabaseInstance
           metabase.models.field.FieldInstance
           metabase.models.table.TableInstance))

;;; ## INTERFACE + CONSTANTS

(def ^:const max-sync-lazy-seq-results
  "The maximum number of values we should return when using `field-values-lazy-seq`.
   This many is probably fine for inferring special types and what-not; we don't want
   to scan millions of values at any rate."
  10000)

(def ^:const max-result-rows
  "Maximum number of rows the QP should ever return."
  10000)

(def ^:const max-result-bare-rows
  "Maximum number of rows the QP should ever return specifically for `rows` type aggregations."
  2000)

(def ^:const connection-error-messages
  "Generic error messages that drivers should return in their implementation of `humanize-connection-error-message`."
  {:cannot-connect-check-host-and-port "Hmm, we couldn't connect to the database. Make sure your host and port settings are correct."
   :database-name-incorrect            "Looks like the database name is incorrect."
   :invalid-hostname                   "It looks like your host is invalid. Please double-check it and try again."
   :password-incorrect                 "Looks like your password is incorrect."
   :password-required                  "Looks like you forgot to enter your password."
   :username-incorrect                 "Looks like your username is incorrect."
   :username-or-password-incorrect     "Looks like the username or password is incorrect."})


(def ^:dynamic *disable-qp-logging*
  "Should we disable logging for the QP? (e.g., during sync we probably want to turn it off to keep logs less cluttered)."
  false)


(defprotocol IDriver
  "Methods that Metabase drivers must implement. Methods marked *OPTIONAL* have default implementations in `IDriverDefaultsMixin`.
   Drivers should also implement `clojure.lang.Named`, so we can call `name` on them:

     (name (PostgresDriver.)) -> \"PostgreSQL\""
  (details-fields ^clojure.lang.Sequential [this]
    "A vector of maps that contain information about connection properties that should
     be exposed to the user for databases that will use this driver. This information is used to build the UI for editing
     a `Database` `details` map, and for validating it on the Backend. It should include things like `host`,
     `port`, and other driver-specific parameters. Each field information map should have the following properties:

       *  `:name`

           The key that should be used to store this property in the `details` map.

       *  `:display-name`

           Human-readable name that should be displayed to the User in UI for editing this field.

       *  `:type` *(OPTIONAL)*

          `:string`, `:integer`, `:boolean`, or `:password`. Defaults to `:string`.

       *  `:default` *(OPTIONAL)*

           A default value for this field if the user hasn't set an explicit value. This is shown in the UI as a placeholder.

       *  `:placeholder` *(OPTIONAL)*

          Placeholder value to show in the UI if user hasn't set an explicit value. Similar to `:default`, but this value is
          *not* saved to `:details` if no explicit value is set. Since `:default` values are also shown as placeholders, you
          cannot specify both `:default` and `:placeholder`.

       *  `:required` *(OPTIONAL)*

          Is this property required? Defaults to `false`.")
  (features ^java.util.Set [this]
    "*OPTIONAL*. A set of keyword names of optional features supported by this driver, such as `:foreign-keys`.")
  (can-connect? ^Boolean [this, ^Map details-map]
    "Check whether we can connect to a `Database` with DETAILS-MAP and perform a simple query. For example, a SQL database might
     try running a query like `SELECT 1;`. This function should return `true` or `false`.")
  (active-tables ^java.util.Set [this, ^DatabaseInstance database]
    "Return a set of maps containing information about the active tables/views, collections, or equivalent that currently exist in DATABASE.
     Each map should contain the key `:name`, which is the string name of the table. For databases that have a concept of schemas,
     this map should also include the string name of the table's `:schema`.")
  (active-column-names->type ^java.util.Map [this, ^TableInstance table]
    "Return a map of string names of active columns (or equivalent) -> `Field` `base_type` for TABLE (or equivalent).")
  (table-pks ^java.util.Set [this, ^TableInstance table]
    "Return a set of string names of active Fields that are primary keys for TABLE (or equivalent).")
  (field-values-lazy-seq ^clojure.lang.Sequential [this, ^FieldInstance field]
    "Return a lazy sequence of all values of FIELD.
     This is used to implement `mark-json-field!`, and fallback implentations of `mark-no-preview-display-field!` and `mark-url-field!`
     if drivers *don't* implement `field-avg-length` and `field-percent-urls`, respectively.")
  (process-query [this, ^Map query]
    "Process a native or structured QUERY. This function is called by `metabase.driver.query-processor/process` after performing various driver-unspecific
     steps like Query Expansion and other preprocessing.")
  (table-fks ^java.util.Set [this, ^TableInstance table]
    "*(REQUIRED FOR DRIVERS THAT SUPPORT `:foreign-keys`)*

     Return a set of maps containing info about FK columns for TABLE.
     Each map should contain the following keys:

       *  `fk-column-name`
       *  `dest-table-name`
       *  `dest-column-name`")
  (active-nested-field-name->type ^java.util.Map [this, ^FieldInstance field]
    "*(REQUIRED FOR DRIVERS THAT SUPPORT `:nested-fields`)*

     Return a map of string names of active child `Fields` of FIELD -> `Field.base_type`.")
  (humanize-connection-error-message ^String [this, ^String message]
    "*OPTIONAL*. Return a humanized (user-facing) version of an connection error message string.
     Generic error messages are provided in the constant `connection-error-messages`; return one of these whenever possible.")
  (sync-in-context [this, ^DatabaseInstance database, ^IFn f]
    "*OPTIONAL*. Drivers may provide this function if they need to do special setup before a sync operation such as `sync-database!`. The sync
     operation itself is encapsulated as the lambda F, which must be called with no arguments.

       (defn sync-in-context [database f]
         (with-jdbc-metadata [_ database]
           (f)))")
  (process-query-in-context [this, ^IFn f]
    "*OPTIONAL*. Similar to `sync-in-context`, but for running queries rather than syncing. This should be used to do things like open DB connections
     that need to remain open for the duration of post-processing. This function follows a middleware pattern and is injected into the QP
     middleware stack immediately after the Query Expander; in other words, it will receive the expanded query.
     See the Mongo and H2 drivers for examples of how this is intended to be used.")
  (table-rows-seq ^clojure.lang.Sequential [this, ^DatabaseInstance database, ^String table-name]
    "*OPTIONAL*. Return a sequence of all the rows in a table with a given TABLE-NAME.
     Currently, this is only used for iterating over the values in a `_metabase_metadata` table. As such, the results are not expected to be returned lazily.")
  (field-avg-length ^Float [this, ^FieldInstance field]
    "*OPTIONAL*. If possible, provide an efficent DB-level function to calculate the average length of non-nil values of textual FIELD, which is used to determine whether a `Field`
     should be marked as a `:category`. If this function is not provided, a fallback implementation that iterates over results in Clojure-land is used instead.")
  (field-percent-urls ^Float [this, ^FieldInstance field]
    "*OPTIONAL*. If possible, provide an efficent DB-level function to calculate what percentage of non-nil values of textual FIELD are valid URLs, which is used to determine
     whether a `Field` should be marked as a `:url`. If this function is not provided, a fallback implementation that iterates over results in Clojure-land is used instead.")
  (driver-specific-sync-field! ^metabase.models.field.FieldInstance [this, ^FieldInstance field]
    "*OPTIONAL*. This is a chance for drivers to do custom `Field` syncing specific to their database.
     For example, the Postgres driver can mark Postgres JSON fields as `special_type = json`.
     As with the other Field syncing functions in `metabase.driver.sync`, this method should return the modified FIELD, if any, or `nil`."))
