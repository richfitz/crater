##' Create a crate client.  The function \code{has_crate} is a small
##' convenience funtion to test if there is a crate server running at
##' a given url.
##'
##' @title Create a crate client
##
##' @param url The URL of the server to connect to.  By default this
##'   will use \code{http://127.0.0.1:4200}.  You will need to include
##'   both the protocol and the port number in most cases.
##
##' @export
##' @author Rich FitzJohn
##' @importFrom R6 R6Class
crate_client <- function(url = NULL) {
  R6_crate_client$new(url)
}

##' @export
##' @rdname crate_client
has_crate <- function(url = NULL) {
  cl <- crate_client(url)
  !is.null(tryCatch(cl$server_info(),
                    error = function(e) NULL))
}

R6_crate_client <- R6::R6Class(
  "crate_client",

  public = list(
    server = NULL,

    initialize = function(url) {
      self$server <- crate_server_handle(url %||% "http://127.0.0.1:4200")
    },

    request = function(...) {
      ## TODO: This exists primarily so we can eventually handle
      ## passing a set servers through to the client (see the python
      ## client for an example).
      self$server$request(...)
    },

    server_info = function() {
      response <- self$request("GET", "/")
      crate_stop_for_status(response)
      ret <- json_from_response(response)
      ret$server <- self$server$url
      ret$version$number <- numeric_version(ret$version$number)
      ret$version$es_version <- numeric_version(ret$version$es_version)
      ret$version$lucene_version <- numeric_version(ret$version$lucene_version)
      ret
    },

    sql = function(statement, parameters = NULL, bulk_parameters = NULL,
                   col_types = FALSE, default_schema = NULL, as = "parsed",
                   verbose = FALSE) {
      crate_sql(statement, self$request, parameters, bulk_parameters,
                col_types, default_schema, as, verbose)
    },

    blob = function(table) {
      ## TODO: assert_scalar_character(table) (or in crate_blob$new)
      crate_blob$new(self, table)
    },

    blob_tables = function() {
      dat <- self$sql("SHOW TABLES IN blob", as = "parsed")
      if (dat$rowcount == 0L) {
        character(0)
      } else {
        unlist(dat$rows)
      }
    }
  ))

## https://crate.io/docs/reference/en/0.55.0/sql/rest.html
crate_sql <- function(statement, request, parameters = NULL,
                      bulk_parameters = NULL, col_types = FALSE,
                      default_schema = NULL, as = "parsed", verbose = FALSE) {
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

  path_sql <- "/_sql"
  response <- request("POST", path_sql, data = data,
                      query = query, headers = headers)
  crate_stop_for_status(response)
  str <- httr::content(response, "text")
  dat <- from_json(str)

  if (verbose) {
    message(sql_message(dat, statement))
  }

  if (as == "tibble") {
    crate_dat_to_df(dat)
  } else if (as == "parsed") {
    dat
  } else { ## string
    str
  }
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
    if (!is.data.frame(bulk_args) || is_json(bulk_args)) {
      stop("Expected a data.frame or json for bulk args")
    }
    data$bulk_args <- bulk_args
  }
  ## TODO: There is some issues here with unboxing, and with dealing
  ## with datetimes that need some serious work
  jsonlite::toJSON(data, "values")
}

sql_message <- function(dat, statement) {
  command <- toupper(sub("\\s*(.*?)\\s.*", "\\1", statement))
  if (is.null(dat$rowcount) && is.list(dat$results)) {
    ## Bulk insert:
    nr <- viapply(dat$results, function(x) as.integer(x$rowcount))
    rowcount <- sprintf("%s (in %s)",
                        plural(sum(nr), "row"),
                        plural(length(nr), "set"))
    ## TODO:
    ## > If an error occures the rowcount is -2 and the result may
    ## > contain an error_message depending on the error.
    ##
    ## From the python client docs
  } else {
    rowcount <- plural(dat$rowcount, "row")
  }
  if (dat$duration > -1) {
    duration <- sprintf("%.3f sec", dat$duration / 1000)
  } else {
    duration <- ""
  }
  if (length(dat$cols) > 0L) {
    msg <- sprintf("%s %s in set %s",
                   command, rowcount, duration)
  } else {
    msg <- sprintf("%s OK, %s affected %s",
                   command, rowcount, duration)
  }
  msg
}
