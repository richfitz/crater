PATH_SQL <- "/_sql"
PATH_BLOB <- "/_blob"

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

  json_request <- function(method, path, data) {
    response <- request(method, path, data = data)
    crate_stop_for_status(response)
    str <- httr::content(response, "text")
    jsonlite::fromJSON(str)
  }

  server_info <- function() {
    response <- request("GET", "/")
    crate_stop_for_status(response)
    content <- json_from_response(response)
    list(server = server$url,
         node_name = content$name,
         node_version = numeric_version(content$version$number))
  }

  ## These bits here are the core API:
  sql <- function(statement, parameters = NULL, bulk_parameters = NULL) {
    if (is.null(statement)) {
      return(NULL)
    }
    data <- create_sql_payload(statement, parameters, bulk_parameters)
    json_request("POST", PATH_SQL, data = data)
  }

  list(close = close,
       sql = sql,
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
