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
  testthat::skip("crate is not running")
}

test_client <- function() {
  skip_if_no_crate()
  crate_client(NULL)
}

SHARED_LOCAL <- "shared"
SHARED_REMOTE <- "/host"

setup_locations_data <- function() {
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

  file.copy("locations.json", SHARED_LOCAL)

  list(c(drop,
         create,
         "delete from locations",
         sprintf("copy locations from '%s/locations.json'", SHARED_REMOTE),
         "refresh table locations"),
       drop)
}

setup_atomic_data <- function() {
  "create table atomic (
          a_byte byte,
          a_boolean boolean,
          a_string string,
          a_ip ip,
          a_double double,
          a_float float,
          a_short short,
          a_integer integer,
          a_long long,
          a_timestamp timestamp)" -> create
  drop <- "drop table if exists atomic"

  file.copy("atomic.json", SHARED_LOCAL)

  list(c(drop,
         create,
         sprintf("copy atomic from '%s/atomic.json'", SHARED_REMOTE),
         "refresh table atomic"),
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
setup_atomic <- function(cl, verbose = FALSE) {
  setup(cl, setup_atomic_data(), verbose)
}

random_table <- function(prefix = "crater_") {
  basename(tempfile(prefix))
}
