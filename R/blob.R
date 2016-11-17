crate_blob <- R6::R6Class(
  "crate_blob",

  public = list(
    crate = NULL,
    table = NULL,

    initialize = function(crate, table) {
      self$crate <- crate
      self$table <- table
    },

    destroy = function() {
      self$crate$sql(sprintf("DROP BLOB TABLE IF EXISTS %s", self$table))
    },

    get = function(digest) {
      response <- self$crate$request("GET", crate_blob_path(self$table, digest))
      handle_response(response,
                      "404" = stop(DigestNotFoundError(self$table, digest)))
      httr::content(response, "raw")
    },

    set = function(data, digest = NULL) {
      assert_raw(data)
      if (is.null(digest)) {
        digest <- hash_sha1(data)
      }
      response <- self$crate$request("PUT", crate_blob_path(self$table, digest),
                               data = data)
      handle_response(response,
                      "201" = TRUE,
                      "409" = FALSE)
    },

    del = function(digest) {
      response <-
        self$crate$request("DELETE", crate_blob_path(self$table, digest))
      handle_response(response,
                      "204" = TRUE,
                      "404" = FALSE)
    },

    exists = function(digest) {
      response <- self$crate$request("HEAD", crate_blob_path(self$table, digest))
      handle_response(response,
                      "200" = TRUE,
                      "404" = FALSE)
    },

    list = function() {
      ## Possible outcomes to guard against here:
      ##
      ## 1. self$table does not exist: throw appropriate error
      ## 2. self$table is empty; return character(0)
      dat <- self$crate$sql(sprintf("SELECT DIGEST FROM blob.%s", self$table),
                            as = "parsed")
      if (dat$rowcount == 0L) {
        character(0)
      } else {
        unlist(dat$rows)
      }
    }
  ))

crate_blob_path <- function(table, digest) {
  sprintf("/_blobs/%s/%s", table, digest)
}
