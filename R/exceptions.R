DigestNotFoundError <- function(table, digest) {
  msg <- sprintf("blob %s not found in table %s", digest, table)
  structure(list(table = table,
                 digest = digest,
                 message = msg,
                 call = NULL),
            class = c("DigestNotFoundError", "error", "condition"))
}
