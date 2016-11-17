test_that("storr - spec", {
  ## TODO: this is in pretty good shape; the core approach works at least.
  ##
  ## Table creation looks very slow and that's not nice; we might need
  ## to create some pseudo-tables here to avoid that.  It might be
  ## worth a bug report or a question on the crate issues.
  ##
  ## - work up a minimal example and decide if it's the blob table or
  ##   SQL table that is slow.
  skip_if_not_installed("storr")
  cl <- test_client()
  create <- function(dr = NULL) {
    if (is.null(dr)) {
      dr <- list(tbl_data = random_table(),
                 tbl_keys = random_table())
    }
    storr_driver_crate(cl, dr$tbl_data, dr$tbl_keys)
  }
  storr:::test_driver(create)
})
