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
  res <- cl$sql("SELECT * from tweets limit 10")
  expect_is(res, "tbl_df")
  expect_equal(nrow(res), 10)

  expect_is(res$created_at, "POSIXct")
  expect_is(res$id, "character")
  expect_is(res$retweeted, "logical")
  expect_is(res$source, "character")
  expect_is(res$text, "character")
  expect_is(res$user, "list")
})
