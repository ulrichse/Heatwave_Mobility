
# Code used to create M6_Hourly_POI_Visits_Adjusted.parquet

## SETUP ----
library(arrow)
library(tidyverse)
library(data.table)

library(lubridate)
library(tidyr)

setwd("C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility/")

## SPECIFY DATASET ----
ds <- open_dataset("Data/Mobility/M5_Hourly_POI_Visits_2022_2024.parquet")

cbgs <- ds %>%
  distinct(poi_cbg) %>%
  collect() %>%
  pull(poi_cbg)

## PIVOT TO DAILY TIME SERIES ----
cols_day <- names(ds)[grepl("^DAY", names(dat))]

out_dir <- "Data/Mobility/cbg_chunks"
dir.create(out_dir, showWarnings = FALSE)

for (cbg in cbgs) {
  
  message("Processing CBG: ", cbg)
  
  dat <- ds %>%
    filter(poi_cbg == cbg) %>%
    select(
      placekey, naics_code, poi_cbg, date_range_start,
      all_of(cols_day)
    ) %>%
    collect()
  
  setDT(dat)
  
  # --- Pivot to daily ---
  dat <- melt(
    dat,
    id.vars = c("placekey", "naics_code", "poi_cbg", "date_range_start"),
    measure.vars = cols_day,
    variable.name = "day_hour",
    value.name = "visits"
  )
  
  dat[, dow := as.integer(sub("DAY", "", tstrsplit(day_hour, "_", fixed = TRUE)[[1]]))]
  dat[, Date := date_range_start + (dow - 1)]
  
  dat <- dat[, .(
    visits = sum(visits, na.rm = TRUE)
  ), by = .(placekey, naics_code, poi_cbg, Date, dow)]
  
  # --- Time features ---
  dat[, year := year(Date)]
  dat[, month := month(Date)]
  
  # --- Detrending ---
  dat[, visits_adj := visits]
  dat[, visits_adj := visits_adj - mean(visits_adj), by = .(placekey, year)]
  dat[, visits_adj := visits_adj - mean(visits_adj), by = .(placekey, month)]
  dat[, visits_detrended := visits_adj - mean(visits_adj), by = .(placekey, dow)]
  dat[, visits_adj := NULL]
  
  # --- Write chunk ---
  write_parquet(
    dat,
    file.path(out_dir, paste0("cbg_", cbg, ".parquet"))
  )
  
  rm(dat)
  gc()
}

final_ds <- open_dataset("Data/Mobility/cbg_chunks")

write_parquet(
  final_ds,
  "Data/Mobility/M6_Daily_POI_Visits_Adjusted.parquet"
)






