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
