
# Code used to create Daily_Patterns_POI_2022_2024.parquet, which takes weekly POI visitation 
# and turns it into a daily time series of visits for each POI


library(tidyverse)
library(arrow)

# Set location of the Safegraph data. For me this is in a Sharepoint folder that
# I added as a shortcut to my file explorer: 

# Use the first one for my desktop and the second one for my laptop:
#data_dir <- "~/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC"
data_dir <- "C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC" 

# Read in weekly visitation patterns 
file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_daily_.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
daily_dat <- file_list %>%
  map_dfr(read_csv)

daily_dat <- daily_dat %>%
  mutate(DAY1_date = as.Date(date_range_start),
         DAY2_date = as.Date(date_range_start + 1),
         DAY3_date = as.Date(date_range_start + 2),
         DAY4_date = as.Date(date_range_start + 3),
         DAY5_date = as.Date(date_range_start + 4),
         DAY6_date = as.Date(date_range_start + 5),
         DAY7_date = as.Date(date_range_start + 6)) %>%
  pivot_longer(
    cols = DAY1:DAY7,
    names_to = "day",
    values_to = "visitor_count"
  ) %>%
  mutate(
    day_num = as.integer(gsub("DAY", "", day)),
    date = date_range_start + (day_num - 1)
  ) %>%
  select(placekey, date, visitor_count)

# Read in poi_info files
poi_dat <- read_parquet("Data/Mobility/POI_Info_Plus_2022_2024.parquet")

# Filter dataframe to avoid many-to-many join
poi_dat <- poi_dat %>%
  distinct(placekey, naics_code, top_category, sub_category)

# Merge poi categories with weekly summary data
daily_ts <- daily_ts %>%
  left_join(poi_dat, by=c("placekey"))

write_parquet(daily_ts, "Data/Mobility/Daily_Patterns_POI_2022_2024.parquet")

