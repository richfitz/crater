assert_raw <- function(x, name = deparse(substitute(x))) {
  if (!is.raw(x)) {
    stop(sprintf("%s must be raw", name), call.=FALSE)
  }
}
