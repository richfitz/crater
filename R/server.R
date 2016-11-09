server_handle <- function(server) {
  ## It is very likely that we might want to do something like:
  ## h <- curl::new_handle(url = server)
  ## curl::handle_setheaders(h,
  ##                         "Accept" = "application/json",
  ##                         "User-Agent" = "crater")
  ## request <- function(method, path, data = NULL, stream = FALSE,
  ##                     headers = NULL) {
  ## }
  ## close <- function() base::close(h)
  ##
  ## and then use curl directly rather than httr, which will likely be
  ## faster and more likely not to randomly break.
  methods <- list(GET = httr::GET, POST = httr::POST, HEAD = httr::HEAD,
                  PUT = httr::PUT, DELETE = httr::DELETE)
  accept_json <- httr::add_headers(Accept = "application/json")
  request <- function(method, path, data = NULL, headers = NULL) {
    ## TODO: send along content-type: application/json here most of
    ## the time too?
    if (!is.null(headers)) {
      headers <- httr::add_headers(.headers = headers)
    }
    url <- paste0(server, path)
    m <- methods[[method]]
    if (is.null(m)) {
      stop("Invalid method ", method)
    }
    m(url, body = data, accept_json, headers)
  }
  close <- function() NULL

  list(url = server,
       request = request,
       close = close)
}
