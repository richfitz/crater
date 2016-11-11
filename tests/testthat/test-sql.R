context("sql")

## TODO:
##
## See:
##   docs/src/crate/tests.py
## for a nice test setup

test_that("basic", {
  cl <- test_client()
  ## I'll need to save some so that we can test this easily, but that
  ## requires getting table ingest working properly!
  res <- cl$sql("SHOW TABLES WHERE table_name = 'tweets'", as = "parsed")
  if (res$rowcount == 0L) {
    skip("no sample data")
  }

  ## First, test that we can get the
  res_json <- cl$sql("SELECT * from tweets limit 10", as = "string")
  expect_is(res_json, "character")

  res_list <- cl$sql("SELECT * from tweets limit 10", as = "parsed")
  expect_equal(class(res_list), "list")

  ## And the actual data:
  res <- cl$sql("SELECT * from tweets limit 10", as = "tibble")
  expect_is(res, "tbl_df")
  expect_equal(nrow(res), 10)

  expect_is(res$created_at, "POSIXct")
  expect_is(res$id, "character")
  expect_is(res$retweeted, "logical")
  expect_is(res$source, "character")
  expect_is(res$text, "character")
  expect_is(res$user, "list")
})

test_that("insert; direct", {
  cl <- test_client()

  res <- cl$sql("create table my_table (foo integer, bar string)",
                as = "parsed", verbose = TRUE)
  on.exit(cl$sql("drop table my_table", as = "parsed"))

  ## from the crate docs:
  ##
  ## > The column list is always ordered alphabetically by column
  ## > name. If the insert columns are omitted, the values in the
  ## > VALUES clauses must correspond to the table columns in that
  ## > order.
  res <- cl$sql("insert into my_table (foo, bar) VALUES (1, 'a'), (2, 'b')",
                as = "parsed")
  expect_equal(res$rowcount, 2)
  ## crate is *eventually consistent* so we need to force update the
  ## table or the read won't work
  res <- cl$sql("refresh table my_table", as = "parsed")

  res <- cl$sql("SELECT * FROM my_table", as = "tibble")
  expect_equal(res, tibble::data_frame(bar = c('a', 'b'), foo = 1:2))

  res <- cl$sql("insert into my_table (foo, bar) VALUES (3, 'c')",
                as = "parsed")
  expect_equal(res$rowcount, 1)
})

test_that("substitute data", {
  cl <- test_client()

  res <- cl$sql("create table my_table (foo integer, bar string)",
                as = "parsed", verbose = TRUE)
  on.exit(cl$sql("drop table my_table", as = "parsed"))

  res <- cl$sql("insert into my_table (foo, bar) VALUES (1, 'a'), (2, 'b')",
                as = "parsed")
  res <- cl$sql("refresh table my_table", as = "parsed", verbose = TRUE)

  for (pat in c("?", "$1")) {
    sql <- sprintf("select * from my_table where foo > %s", pat)
    res <- cl$sql(sql, parameters = 1, as = "tibble")
    expect_equal(res$foo, 2)

    res <- cl$sql(sql, parameters = -1, as = "tibble")
    v <- c("a", "b")
    expect_true(setequal(res$bar, v))
    i <- match(v, res$bar)
    expect_equal(res$foo[i], 1:2)
  }
})

test_that("bulk insert", {
  cl <- test_client()

  cleanup <- setup_locations(cl)
  on.exit(cleanup())

  df <- data.frame(id = 1337:1339,
                   name = c("Earth", "Sun", "Titan"),
                   kind = c("Planet", "Star", "Moon"),
                   description = c(
                     "An awesome place to spend some time on.",
                     "An extraordinarily hot place.",
                     "Titan, where it rains fossil fuels."),
                   stringsAsFactors = FALSE)
  sql <- "INSERT INTO locations (id, name, kind, description)
          VALUES (?, ?, ?, ?)"
  options(error = recover)
  res <- cl$sql(sql, bulk_parameters = df, verbose = TRUE)
  cl$sql("refresh table locations")

  r <- cl$sql("SELECT * from locations WHERE name = 'Earth'", as = "tibble",
              verbose = TRUE)
  expect_equal(nrow(r), 1)
  expect_equal(r$id, "1337")
})

test_that("types: crate -> R", {
  cl <- test_client()

  cleanup <- setup_atomic(cl)
  on.exit(cleanup())

  ## Empty result, still get the correct types
  dat <- cl$sql("SELECT * FROM atomic WHERE false", as = "tibble")
  expect_equal(nrow(dat), 0L)
  expect_is(dat$a_byte, "raw")
  expect_is(dat$a_boolean, "logical")
  expect_is(dat$a_string, "character")
  expect_is(dat$a_ip, "character") # TODO
  expect_is(dat$a_double, "numeric")
  expect_is(dat$a_float, "numeric")
  expect_is(dat$a_short, "integer")
  expect_is(dat$a_integer, "integer")
  expect_is(dat$a_long, "integer")
  expect_is(dat$a_timestamp, "POSIXct")

  dat <- cl$sql("SELECT * FROM atomic", as = "tibble")

  cmp <- lapply(readLines("atomic.json"), jsonlite::fromJSON)
  cmp1 <- cmp[[1]]

  expect_true(setequal(names(cmp1), names(dat)))
  expect_equal(dat$a_byte, as.raw(cmp1$a_byte))
  expect_equal(dat$a_boolean, cmp1$a_boolean)
  expect_equal(dat$a_string, cmp1$a_string)
  expect_equal(dat$a_ip, cmp1$a_ip) # TODO
  expect_equal(dat$a_double, cmp1$a_double)
  expect_equal(dat$a_float, cmp1$a_float)
  expect_equal(dat$a_short, cmp1$a_short)
  expect_equal(dat$a_integer, cmp1$a_integer)
  expect_equal(dat$a_long, cmp1$a_long)
  expect_equal(dat$a_timestamp,
               as.POSIXct(cmp1$a_timestamp / 1000, "UTC",
                          origin = "1970-01-01"))
})
