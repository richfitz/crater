## https://crate.io/docs/reference/en/0.55.0/sql/rest.html

PATH_SQL <- "/_sql"
path_blob <- function(table, digest) {
  sprintf("/_blobs/%s/%s", table, digest)
}

## I'm unsure if this will move into being R6 or not.  It probably
## will...
client <- function(url = NULL) {
  server <- server_handle(url %||% "http://127.0.0.1:4200")

  close <- function() {
    lapply(server_pool, function(s) s$close())
    invisible(NULL)
  }

  request <- function(method, path, ...) {
    ## This needs a bunch of work for the clustered case where we'll
    ## be passed redirects here.
    server$request(method, path, ...)
  }

  server_info <- function() {
    response <- request("GET", "/")
    crate_stop_for_status(response)
    ret <- json_from_response(response)
    ret$server <- server$url
    ret$version$number <- numeric_version(ret$version$number)
    ret$version$es_version <- numeric_version(ret$version$es_version)
    ret$version$lucene_version <- numeric_version(ret$version$lucene_version)
    ret
  }

  ## sql is one of the main endpoints
  sql <- function(statement, parameters = NULL, bulk_parameters = NULL,
                  col_types = FALSE, default_schema = NULL, as = "parsed") {
    as <- match.arg(as, c("string", "parsed", "tibble"))
    if (is.null(statement)) {
      return(NULL)
    }
    if (col_types || as == "tibble") {
      query <- list(types = "")
    } else {
      query <- NULL
    }
    if (!is.null(default_schema)) {
      headers <- c("Default-Schema" = default_schema)
    } else {
      headers <- NULL
    }

    data <- create_sql_payload(statement, parameters, bulk_parameters)

    response <- request("POST", PATH_SQL, data = data,
                        query = query, headers = headers)
    crate_stop_for_status(response)
    str <- httr::content(response, "text")

    if (as == "tibble") {
      crate_json_to_df(str)
    } else if (as == "parsed") {
      from_json(str)
    } else { ## string
      str
    }
  }

  ## Blob support: Get, Set, Del, Exists, List
  ##
  ## The main (only?) things that get found by scope here are
  ## 'request()'
  blob_get <- function(table, digest) {
    response <- request("GET", path_blob(table, digest))
    ## TODO: could roll this into the stop for status with various
    ## handlers, lazily evaluated.
    if (httr::status_code(response) == 404) {
      stop(DigestNotFoundError(table, digest))
    }
    ## TODO: could use curl_fetch_stream here, which is in the new curl
    crate_stop_for_status(response)
    httr::content(response, "raw")
  }
  blob_set <- function(table, digest, data) {
    assert_raw(data)
    response <- request("PUT", path_blob(table, digest), data = data)
    code <- httr::status_code(response)
    if (code == 201) {
      return(TRUE)
    } else if (code == 409) {
      return(FALSE)
    } else {
      ## TODO: special treatment for the 400 error here which the
      ## python lib throws as BlobsDisabledException
      crate_stop_for_status(response)
    }
  }
  blob_del <- function(table, digest) {
    response <- request("DELETE", path_blob(table, digest))
    code <- httr::status_code(response)
    if (code == 204) {
      return(TRUE)
    } else if (code == 404) {
      return(FALSE)
    } else {
      crate_stop_for_status(response)
    }
  }
  blob_exists <- function(table, digest) {
    response <- request("HEAD", path_blob(table, digest))
    code <- httr::status_code(response)
    if (code == 200) {
      return(TRUE)
    } else if (code == 404) {
      return(FALSE)
    } else {
      crate_stop_for_status(response)
    }
  }
  blob_list <- function(table) {
    ## Possible outcomes to guard against here:
    ##
    ## 1. table does not exist: throw appropriate error
    ## 2. table is empty; return character(0)
    dat <- sql(sprintf("SELECT DIGEST FROM blob.%s", table), as = "parsed")
    if (dat$rowcount == 0L) {
      character(0)
    } else {
      unlist(dat$rows)
    }
  }
  blob_tables <- function() {
    dat <- sql("SHOW TABLES IN blob", as = "parsed")
    if (dat$rowcount == 0L) {
      character(0)
    } else {
      unlist(dat$rows)
    }
  }

  blob <- list(get = blob_get,
               set = blob_set,
               del = blob_del,
               exists = blob_exists,
               list = blob_list,
               tables = blob_tables)

  list(close = close,
       sql = sql,
       blob = blob,
       request = request,
       server_info = server_info)
}

create_sql_payload <- function(statement, args, bulk_args) {
  data <- list(stmt = jsonlite::unbox(statement))
  if (!is.null(args)) {
    data$args <- args
    if (!is.null(bulk_args)) {
      stop("Cannot provide both: args and bulk_args")
    }
  }
  if (!is.null(bulk_args)) {
    data$bulk_args <- bulk_args
  }
  ## TODO: There is some issues here with unboxing, and with dealing
  ## with datetimes that need some serious work
  jsonlite::toJSON(data)
}

has_crate <- function(url = NULL) {
  cl <- client(url)
  !is.null(tryCatch(cl$server_info(),
                    error = function(e) NULL))
}
