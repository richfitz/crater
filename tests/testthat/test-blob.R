context("blob")

test_that("basic support", {
  cl <- client()

  tbl <- basename(tempfile("blob_"))
  res <- cl$sql(sprintf("CREATE BLOB TABLE %s", tbl))

  expect_true(tbl %in% cl$blob$tables())
  expect_equal(cl$blob$list(tbl), character(0))

  dat <- random_bytes(10)
  sha <- hash_sha1(dat)
  expect_false(cl$blob$exists(tbl, sha))
  expect_error(cl$blob$get(tbl, sha), "not found in table")
  expect_false(cl$blob$del(tbl, sha))

  expect_true(cl$blob$set(tbl, sha, dat))
  expect_false(cl$blob$set(tbl, sha, dat))
  expect_true(cl$blob$exists(tbl, sha))
  expect_equal(cl$blob$get(tbl, sha), dat)
  expect_equal(cl$blob$list(tbl), sha)

  expect_true(cl$blob$del(tbl, sha))
  expect_false(cl$blob$del(tbl, sha))
  expect_false(cl$blob$exists(tbl, sha))
  expect_error(cl$blob$get(tbl, sha), "not found in table")

  res <- cl$sql(sprintf("DROP BLOB TABLE %s", tbl))
  expect_false(tbl %in% cl$blob$tables())
})
