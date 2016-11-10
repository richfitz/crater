context("sql")

test_that("basic", {
  skip_if_no_crate()
  ## I'll need to save some so that we can test this easily, but that
  ## requires getting table ingest working properly!
  cl <- client()
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
  skip_if_no_crate()
  cl <- client()

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
  skip_if_no_crate()
  cl <- client()

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
