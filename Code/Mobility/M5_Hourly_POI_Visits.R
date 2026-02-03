# Code used to create Hourly_Patterns_POI_2022_2024.parquet, which takes weekly POI visitation 
# and turns it into a daily time series of visits for each POI

## LOAD LIBRARIES ----
library(tidyverse)
library(arrow)

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
)

# Keep weeks that overlap the warm season (May to Sept)
file_dates_filt <- file_dates %>%
  filter(
    month(week_start) <= 9,
    month(week_end)   >= 5
  )

## MERGE DAILY WARM SEASON DATA ----
# Read only those files and combine into a single dataframe
hourly_dat <- file_dates_filt$file %>%
  map_dfr(read_csv)
rm(file_dates)
rm(file_dates_filt)
gc()
## SLICE HOURS INTO DAYS & PERIODS ----
hourly_dat <- hourly_dat %>%
  mutate(
    !!!{
      days <- 1:7
      blocks <- list(
        HOUR1_HOUR8   = 1:8, # morning period
        HOUR9_HOUR16  = 9:16, # daytime period
        HOUR17_HOUR24 = 17:24 # evening/night period
      )
      
      out <- list()
      
      for (d in days) {
        day_offset <- (d - 1) * 24
        
        for (b in names(blocks)) {
          hrs <- blocks[[b]] + day_offset
          cols <- paste0("HOUR", hrs)
          
          out[[paste0("DAY", d, "_", b)]] <-
            expr(rowSums(across(all_of(cols)), na.rm = TRUE))
        }
      }
      out
    }
  )

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







