context("crate")

test_that("server_info", {
  cl <- test_client()
  dat <- cl$server_info()

  expect_equal(dat$server, "http://127.0.0.1:4200")
  expect_is(dat$name, "character")
  expect_is(dat$version$number, "numeric_version")
})
