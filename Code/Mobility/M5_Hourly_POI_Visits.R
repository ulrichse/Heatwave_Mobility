
# Code used to create Hourly_Patterns_POI_2022_2024.parquet, which takes weekly POI visitation 
# and turns it into a daily time series of visits for each POI

## LOAD LIBRARIES ----
library(tidyverse)
library(arrow)
library(data.table)

## READ IN DATASETS ----

# Set location of the Safegraph data. For me this is in a Sharepoint folder that
# I added as a shortcut to my file explorer: 

# Use the first one for my desktop and the second one for my laptop:
# Sorry Paul
#### SET DATA DIRECTORY ----
data_dir <- "~/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC"
#data_dir <- "C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC" 

#### CREATE FILE LISTS ----
file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_hourly_.*\\.csv$", full.names = TRUE)

#### FILTER DATES TO WARM SEASON ----
file_dates <- tibble(
  file = file_list,
  week_start = ymd(str_extract(file_list, "\\d{4}-\\d{2}-\\d{2}")),
  week_end   = week_start + days(6)
) %>%
# Keep weeks that overlap the warm season (May to Sept)
# This will include weeks that begin in April, but end in May
# or weeks that begin in September, but end in October.
# (Note that in the conversion to a daily time series, these non-warm season 
# days will be dropped)
  filter(
    month(week_start) <= 9,
    month(week_end)   >= 5
  )

## MERGE DAILY WARM SEASON DATA ----
# Read only those warm season files and combine into a single dataframe
hourly_dat <- file_dates$file %>%
  map_dfr(read_csv)
rm(file_dates)
gc()

## SLICE HOURS INTO DAYS & PERIODS ----

setDT(hourly_dat)

for (d in 1:7) {
  day_offset <- (d - 1) * 24
  
  hourly_dat[, paste0("DAY", d, "_HOUR1_HOUR8") :=
               rowSums(.SD[, 5:13 + day_offset, with = FALSE], na.rm = TRUE)]
  
  hourly_dat[, paste0("DAY", d, "_HOUR9_HOUR16") :=
               rowSums(.SD[, 14:21 + day_offset, with = FALSE], na.rm = TRUE)]
  
  hourly_dat[, paste0("DAY", d, "_HOUR17_HOUR24") :=
               rowSums(.SD[, 22:29 + day_offset, with = FALSE], na.rm = TRUE)]
}

#### FILTER ----
hourly_dat <- hourly_dat %>%
  select(placekey, date_range_start, raw_visitor_counts, raw_visit_counts, starts_with("DAY"))

## ADD POI INFO ----
poi_info <- read_parquet("Data/Mobility/POI_Info_Plus_2022_2024.parquet") %>%
  distinct(placekey, naics_code, top_category, sub_category, poi_cbg) # Avoids many-to-many join

hourly_dat <- hourly_dat %>%
  left_join(poi_info, by = "placekey")
rm(poi_info)
gc()

write_parquet(hourly_dat, "Data/Mobility/M5_Hourly_POI_Visits_2022_2024.parquet")

rm(hourly_dat)







