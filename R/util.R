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

is_json <- function(x) {
  inherits(x, "json")
}

## regular plurals only!
plural <- function(n, name) {
  ngettext(n, paste(1, name), sprintf("%d %ss", n, name))
}
