random_bytes <- function(n) {
  sample(as.raw(0:255), n, replace = TRUE)
}

random_string <- function(n) {
  pos <- as.raw(32:126)
  rawToChar(sample(pos, n, replace = TRUE))
}

skip_if_no_crate <- function() {
  if (has_crate()) {
    return()
  }
  testthat::skip("crate was not running")
}

test_client <- function() {
  skip_if_no_crate()
  client(NULL)
}

setup_locations_data <- function() {
  shared_local <- "shared"
  shared_remote <- "/host"

  "create table locations (
          id string primary key,
          name string,
          \"date\" timestamp,
          kind string,
          position integer,
          description string,
          race object(dynamic) as (
            interests array(string),
            description string,
            name string
          ),
          information array(object as (
              population long,
              evolution_level short
            )
          ),
          index name_description_ft using fulltext(name, description) with (analyzer='english')
        ) clustered by(id) into 2 shards with (number_of_replicas=0)" -> create
  drop <- "drop table if exists locations"

  file.copy("locations.json", shared_local)

  list(c(drop,
         create,
         "delete from locations",
         sprintf("copy locations from '%s/locations.json'", shared_remote),
         "refresh table locations"),
       drop)
}

setup <- function(cl, data, verbose = FALSE) {
  for (sql in data[[1L]]) {
    cl$sql(sql, verbose = verbose)
  }
  function(verbose = FALSE) {
    for (sql in data[[2L]]) {
      cl$sql(sql, verbose = verbose)
    }
  }
}

setup_locations <- function(cl, verbose = FALSE) {
  setup(cl, setup_locations_data(), verbose)
}
