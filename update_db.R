# Packages
library(microdatasus)
library(DBI)
library(recbilis)
library(dplyr)
library(lubridate)
library(tidyr)
library(glue)


# Database connection
conn <- recbilis:::db_connect()
# dbListObjects(conn, Id(schema = "records"))

# Health region
load(file = "mun_reg_saude_449.rda")

# Definitions
years <- 2010:2024

# Dengue

## Database cleaning
if (dbExistsTable(conn, Id(schema = "records", table = "dengue"))) {
  dbRemoveTable(conn, Id(schema = "records", table = "dengue"))
}

## Download
for (y in years) {
  message(Sys.time())
  message(glue("Ano: {y}"))

  # Download data
  tmp <- fetch_datasus(
    year_start = y,
    year_end = y,
    information_system = "SINAN-DENGUE",
    timeout = 10000
  ) |>
    # Pre-process data
    process_sinan_dengue() |>
    # Select and rename fields
    select(
      geocodmu = ID_MN_RESI,
      date = DT_SIN_PRI,
      sex = CS_SEXO,
      age = IDADEanos
    ) |>
    # Fix schema and add fields
    mutate(
      geocodmu = as.integer(substr(geocodmu, 0, 6)),
      geocoduf = substr(geocodmu, 0, 2),
      date = as.Date(date),
      year = as.integer(year(date)),
      month = as.integer(month(date)),
      epiweek = as.integer(epiweek(date)),
      health = "Dengue",
      source = "SINAN",
      age = as.integer(age)
    ) |>
    # Update fields
    mutate(
      sex = case_match(
        sex,
        "Feminino" ~ "F",
        "Masculino" ~ "M",
        .default = NA
      )
    ) |>
    replace_na(list(age = 0)) |>
    # Add health region
    left_join(mun_reg_saude_449, by = c("geocodmu" = "cod_mun")) |>
    rename(geocodrs = cod_reg_saude) |>
    mutate(
      geocodrs = as.integer(geocodrs),
      geocodmu = as.integer(geocodmu)
    ) |>
    # Reorder fields
    select(
      geocodmu,
      year,
      month,
      epiweek,
      date,
      health,
      sex,
      age,
      source,
      geocoduf,
      geocodrs
    ) |>
    # Filter out records with invalid dates
    filter(year >= years[1])

  # Write to database
  message("Writing to database...")
  dbWriteTable(
    conn = conn,
    name = Id(schema = "records", table = "dengue"),
    value = tmp,
    append = TRUE
  )

  # Remove tmp object
  rm(tmp)

  message("Done!")
  message(Sys.time())
}
