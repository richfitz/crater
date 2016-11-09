random_bytes <- function(n) {
  sample(as.raw(0:255), n, replace = TRUE)
}

random_string <- function(n) {
  pos <- as.raw(32:126)
  rawToChar(sample(pos, n, replace = TRUE))
}
