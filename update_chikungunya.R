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
years <- 2015:2024
schema_name <- "records"
table_name <- "chikungunya"
health_name <- "chikungunya"
source_name <- "SINAN"

# Chikungunya

## Database cleaning
if (dbExistsTable(conn, Id(schema = schema_name, table = table_name))) {
  dbRemoveTable(conn, Id(schema = schema_name, table = table_name))
}

## Download
for (y in years) {
  message(Sys.time())
  message(glue("Ano: {y}"))

  # Download data
  tmp <- fetch_datasus(
    year_start = y,
    year_end = y,
    information_system = "SINAN-CHIKUNGUNYA"
  ) |>
    # Select fields
    select(
      ID_MN_RESI,
      DT_SIN_PRI,
      CS_SEXO,
      NU_IDADE_N,
      CLASSI_FIN
    ) |>
    # Filter positive classifications
    filter(!(CLASSI_FIN %in% c("5", "8"))) |>
    select(-CLASSI_FIN) |>
    # Pre-process data
    process_sinan_chikungunya() |>
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
      health = health_name,
      source = source_name,
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
    name = Id(schema = schema_name, table = table_name),
    value = tmp,
    append = TRUE
  )

  # Remove tmp object
  rm(tmp)

  message("Done!")
  message(Sys.time())
  message("---")
}

dbDisconnect(conn)
