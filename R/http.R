crate_stop_for_status <- function(response) {
  code <- httr::status_code(response)
  if (code < 400) {
    return()
  }
  msg <- httr::http_status(code)$message
  if (httr::http_type(response) == "application/json") {
    data <- jsonlite::fromJSON(httr::content(response, "text"))
    ## This could be further refined, I'm sure.  S3 methods would be
    ## particularly nice to get in here.
    error <- data$error
    if (!is.null(error$message)) {
      stop(error$message)
    }
  }
  stop(msg)
}

json_from_response <- function(response) {
  jsonlite::fromJSON(httr::content(response, "text"))
}

handle_response <- function(x, ...) {
  code <- as.character(httr::status_code(x))
  switch(code, ..., crate_stop_for_status(x))
}
