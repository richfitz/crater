## Storr support for crate
##
## The issue with getting this to work well is that we really want the
## "eventually consistent" thing to work for us.  So it'd be nice to
## be allow the storr to sync, unsync separately.

storr_crate <- function(crate, tbl_data, tbl_keys,
                        default_namespace = "objects") {
  storr::storr(storr_driver_crate(crate, tbl_data, tbl_keys),
               default_namespace)
}

storr_driver_crate <- function(crate, tbl_data, tbl_keys) {
  R6_storr_driver_crate$new(crate, tbl_data, tbl_keys)
}

R6_storr_driver_crate <- R6::R6Class(
  "storr_driver_crate",

  ## It's not really clear here where we should store the
  ## configuration; it could be an additional table (which seems
  ## silly).  Alternatively we could stuff a value in with a special
  ## data key.  I'm thinking that
  public = list(
    sql = NULL,
    blob = NULL,
    tbl_data = NULL,
    tbl_keys = NULL,
    traits = list(accept_raw = TRUE,
                  drop_r_version = TRUE,
                  throw_missing = TRUE,
                  hash_algorithm = FALSE),
    hash_algorithm = "sha1",

    initialize=function(crate, tbl_data, tbl_keys) {
      self$tbl_data <- tbl_data
      self$tbl_keys <- tbl_keys

      ## Initialise the tables
      ##
      ## (TODO: I don't see if not exists working here; this is
      ## arguably a bug in crate, and once it is fixed replace this as
      ## it's not pretty).
      sql <- c("SELECT table_name FROM information_schema.tables WHERE",
               sprintf("schema_name = 'blob' AND table_name = '%s'", tbl_data))
      res <- crate$sql(paste(sql, collapse = "\n"), as = "parsed")
      if (res$rowcount == 0L) {
        crate$sql(sprintf("CREATE BLOB TABLE %s", tbl_data))
      }

      sql <- c(sprintf("CREATE TABLE IF NOT EXISTS %s", tbl_keys),
               "(namespace STRING NOT NULL,",
               "key STRING NOT NULL,",
               "hash STRING NOT NULL,",
               "PRIMARY KEY (namespace, key))")
      crate$sql(paste(sql, collapse = "\n"))
      self$sql <- crate$sql
      self$blob <- crate$blob(tbl_data)
      ## NOTE: could lock all the bindings like so:
      ## lockBinding(as.name("hash_algorithm"), self)
    },

    type = function() {
      "crate"
    },

    ## Total destruction of the driver; delete all data stored in both
    ## tables, then delete our database connection to render the
    ## driver useless.
    destroy = function() {
      self$blob$destroy()
      self$sql(sprintf("DROP TABLE %s", self$tbl_keys))
      self$sql <- NULL
      self$blob <- NULL
    },

    ## Return the hash value given a key/namespace pair
    get_hash = function(key, namespace) {
      sql <- sprintf("SELECT hash FROM %s WHERE namespace = $1 AND key = $2",
                     self$tbl_keys)
      data <- c(namespace, key)
      self$sql(sql, data, as = "parsed")$rows[[1L]][[1L]]
    },

    ## Set the key/namespace pair to a hash
    set_hash = function(key, namespace, hash) {
      sql <- c(sprintf("INSERT INTO %s", self$tbl_keys),
               "(namespace, key, hash) VALUES ($1, $2, $3)",
               "ON DUPLICATE KEY UPDATE hash = $3")
      data <- c(namespace, key, hash)
      self$sql(paste(sql, collapse = "\n"), data)
      self$refresh()
    },

    ## Return a (deserialised) R object, given a hash
    get_object = function(hash) {
      unserialize(self$blob$get(hash))
    },

    ## Set a (serialised) R object against a hash.
    set_object = function(hash, value) {
      assert_raw(value)
      self$blob$set(value, hash)
      ## TODO: this doesn't work:
      ##   self$sql(sprintf("refresh table %s", self$tbl_data))
      ## also as refresh blob table and as refresh table blob.%s
      ## Is this needed?
    },

    ## Check if a key/namespace pair exists.
    exists_hash = function(key, namespace) {
      sql <- sprintf("SELECT hash FROM %s WHERE namespace = $1 AND key = $2",
                     self$tbl_keys)
      data <- c(namespace, key)
      self$sql(sql, data, as = "parsed")$rowcount > 0L
    },

    ## Check if a hash exists
    exists_object = function(hash) {
      self$blob$exists(hash)
    },

    ## Delete a key.  Because of the requirement to return TRUE/FALSE on
    ## successful/unsuccessful key deletion this includes an exists_hash()
    ## step first.
    del_hash = function(key, namespace) {
      if (self$exists_hash(key, namespace)) {
        data <- c(namespace, key)
        sql <- sprintf("DELETE FROM %s WHERE namespace = $1 AND key = $2",
                       self$tbl_keys)
        data <- c(namespace, key)
        self$sql(sql, data, as = "parsed")
        self$refresh()
        TRUE
      } else {
        FALSE
      }
    },

    ## Delete a hash
    del_object = function(hash) {
      self$blob$del(hash)
    },

    ## List hashes, namespaces and keys.  Because the SQLite driver seems to
    ## return numeric(0) if the result set is empty, we need as.character here.
    list_hashes = function() {
      self$blob$list()
    },

    list_namespaces = function() {
      sql <- sprintf("SELECT DISTINCT namespace FROM %s", self$tbl_keys)
      self$sql(sql, as = "tibble")$namespace
    },

    list_keys = function(namespace) {
      sql <- sprintf("SELECT key FROM %s WHERE namespace = $1", self$tbl_keys)
      self$sql(sql, namespace, as = "tibble")$key
    },

    refresh = function() {
      self$sql(sprintf("REFRESH TABLE %s", self$tbl_keys))
    }
  ))
