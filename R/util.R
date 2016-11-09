## Temporary stopgap, until I decide if httr will be kept
http_method <- function(type) {
  switch(type,
         GET = httr::GET,
         POST = httr::GET,
         stop("Invalid method ", type))
}

`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}

hash_sha1 <- function(x) {
  assert_raw(x)
  paste(unclass(openssl::sha1(x)), collapse = "")
}

vlapply <- function(X, FUN, ...) {
  vapply(X, FUN, logical(1), ...)
}
viapply <- function(X, FUN, ...) {
  vapply(X, FUN, integer(1), ...)
}
vnapply <- function(X, FUN, ...) {
  vapply(X, FUN, numeric(1), ...)
}
vcapply <- function(X, FUN, ...) {
  vapply(X, FUN, character(1), ...)
}
