context("blob")

test_that("basic support", {
  cl <- test_client()

  tbl <- basename(tempfile("blob_"))
  res <- cl$sql(sprintf("CREATE BLOB TABLE %s", tbl), as = "parsed")
  on.exit(cl$sql(sprintf("DROP BLOB TABLE IF EXISTS %s", tbl), as = "parsed"))

  expect_true(tbl %in% cl$blob_tables())
  bl <- cl$blob(tbl)
  expect_equal(bl$list(), character(0))

  dat <- random_bytes(10)
  sha <- hash_sha1(dat)
  expect_false(bl$exists(sha))
  expect_error(bl$get(sha), "not found in table")
  expect_false(bl$del(sha))

  expect_true(bl$set(dat, sha))
  expect_false(bl$set(dat, sha))
  expect_true(bl$exists(sha))
  expect_equal(bl$get(sha), dat)
  expect_equal(bl$list(), sha)

  expect_true(bl$del(sha))
  expect_false(bl$del(sha))
  expect_false(bl$exists(sha))
  expect_error(bl$get(sha), "not found in table")

  res <- cl$sql(sprintf("DROP BLOB TABLE %s", tbl), as = "parsed")
  expect_false(tbl %in% cl$blob_tables())
})
