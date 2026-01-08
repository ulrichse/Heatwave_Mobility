# OUTPUT FILE: Weekly_Patterns_POI_2022_2024.parquet

library(dplyr)
library(stringr)
library(purrr)
library(readr)
library(ggplot2)
library(sf)
library(tigris)
library(arrow)

setwd("Heatwave_Mobility/")

# Set location of the Safegraph data. For me this is in a Sharepoint folder that
# I added as a shortcut to my file explorer: 

data_dir <- "C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC"

# Read in weekly visitation patterns 
file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_summary.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
dat <- file_list %>%
  map_dfr(read_csv)

# Filter dataframe to avoid many-to-many join
poi_dat <- read_parquet("Data/Mobility/POI_Info_Plus_2022_2024.parquet")
distinct(placekey, naics_code, top_category, sub_category)

# Merge poi categories with weekly summary data
dat <- dat %>%
  left_join(poi_dat, by=c("placekey"))

# Write off the file:  
write_parquet(dat, "Data/Mobility/Weekly_POI_Visits_2022_2024.parquet")