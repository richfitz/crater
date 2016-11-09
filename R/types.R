## https://crate.io/docs/reference/sql/data_types.html

## As of version 0.55.0:
##
## Id	Data Type
## 0	Null
## 1	Not Supported
## 2	Byte
## 3	Boolean
## 4	String
## 5	Ip
## 6	Double
## 7	Float
## 8	Short
## 9	Integer
## 10	Long
## 11	Timestamp
## 12	Object
## 13	GeoPoint (Double[])
## 14	GeoShape
## 100	Array
## 101	Set

## Quite a bit of work will probably be needed to understand the
## compound data types properly (object, array, set) and the
## geographic ones.  So we'll just leave those as character for now.
##
## The conversion here probably wants to occur in C if this is too
## slow.

## TODO: for now (at least) I am using tibble to construct the
## data.frame's as they might contain list columns.  This could be
## done with base functions and I will revisit this.

crate_timestamp_in <- function(x) {
  as.POSIXct(x / 1000, origin = "1970-01-01", tz = "UTC")
}
crate_long_in <- function(x) {
  if (all(abs(x) < .Machine$integer.max)) {
    as.integer(x)
  } else {
    as.numeric(x)
  }
}

## Stick it all together:
crate_types <- do.call("rbind", list(
  ## Also need R -> Crate conversion functions here too
  list(0L, "Null", NA_character_, FALSE, NULL, NULL, NULL),
  list(1L, "Not Supported", NA_character_, FALSE, NULL, NULL, NULL),
  list(2L, "Byte", "raw", TRUE, as.raw, raw(1), NULL),
  list(3L, "Boolean", "logical", TRUE, as.logical, logical(1), NULL),
  list(4L, "String", "character", TRUE, as.character, character(1), NULL),
  list(5L, "Ip", "ip", TRUE, as.character, character(1), NULL), # could support
  list(6L, "Double", "numeric", TRUE, as.numeric, numeric(1), NULL),
  list(7L, "Float", "numeric", TRUE, as.numeric, numeric(1), NULL),
  list(8L, "Short", "Short", TRUE, as.numeric, numeric(1), NULL),
  list(9L, "Integer", "integer", TRUE, as.integer, integer(1), NULL),
  list(10L, "Long", "integer", TRUE, crate_long_in, integer(1), NULL),
  list(11L, "Timestamp", "POSIXct", TRUE, crate_timestamp_in, NULL, NULL),
  list(12L, "Object", "list", FALSE, identity, NULL, NULL),
  list(13L, "GeoPoint (Double[])", NA_character_, FALSE, NULL, NULL, NULL),
  list(14L, "GeoShape", NA_character_, FALSE, NULL, NULL, NULL),
  list(100L, "Array", NA_character_, FALSE, NULL, NULL, NULL),
  list(101L, "Set", NA_character_, FALSE, NULL, NULL, NULL)))

crate_types <- tibble::data_frame(
    id = vapply(crate_types[, 1], identity, integer(1)),
    type_crate = vapply(crate_types[, 2], identity, character(1)),
    type_r = vapply(crate_types[, 3], identity, character(1)),
    atomic = vapply(crate_types[, 4], identity, logical(1)),
    c2r = crate_types[, 5],
    storage = crate_types[, 6],
    r2c = crate_types[, 7])

crate_column <- function(i, rows, col_types) {
  j <- match(col_types[[i]], crate_types$id)
  x <- lapply(rows, "[[", i)
  if (crate_types$atomic[[j]]) {
    stopifnot(all(lengths(x) == 1))
    x <- unlist(x)
  }
  fn <- crate_types$c2r[[j]]
  if (is.null(fn)) x else fn(x)
}

## This whole bit is a work in progress!
crate_json_to_df <- function(str) {
  dat <- jsonlite::fromJSON(str)
  ret <- lapply(seq_along(dat$cols), crate_column, dat$rows, dat$col_types)
  names(ret) <- dat$cols
  tibble::as_data_frame(ret)
}
