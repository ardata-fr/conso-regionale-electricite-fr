library(curl)
library(arrow)
library(tidyverse)
library(duckdb)
library(DBI)

dir.create("data", showWarnings = FALSE, recursive = TRUE)

tf <- tempfile(pattern = "conso-elec", fileext = ".csv")

curl_download(
  url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
  destfile = tf
)

conso <- arrow::read_delim_arrow(file = tf, delim = ";") |>
  rename(
    date_heure = `Date - Heure`,
    date = Date,
    heure = Heure,
    code_insee_region = `Code INSEE région`,
    region = `Région`,
    consommation_brute_gaz_grtgaz = `Consommation brute gaz (MW PCS 0°C) - GRTgaz`,
    statut_grtgaz = `Statut - GRTgaz`,
    consommation_brute_gaz_terega = `Consommation brute gaz (MW PCS 0°C) - Teréga`,
    statut_terega = `Statut - Teréga`,
    consommation_brute_gaz_totale = `Consommation brute gaz totale (MW PCS 0°C)`,
    consommation_brute_electricite_rte = `Consommation brute électricité (MW) - RTE`,
    statut_rte = `Statut - RTE`,
    consommation_brute_totale = `Consommation brute totale (MW)`
  )

canard_con <- dbConnect(duckdb(), dbdir = "data/conso.duckdb")

dbWriteTable(conn = canard_con, name = "conso", value = conso)

dbDisconnect(canard_con, shutdown = TRUE)
