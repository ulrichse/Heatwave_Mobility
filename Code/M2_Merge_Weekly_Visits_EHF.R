
# INPUT: Weekly_Patterns_POI_2022_2024.parquet
# OUTPUT: Weekly_Series_Visits_HW_Merge_2022_2024.parquet

# Merge weekly POI data with daily heatwave data


library(dplyr)
library(tigris)
library(arrow)
library(lubridate)
library(readxl)
library(stringr)
library(tidycensus)
library(data.table)

#setwd("~/Heatwave_Mobility/")


# Read in mobility data: 
dat <- read_parquet("Data/Mobility/Weekly_Patterns_POI_2022_2024.parquet") %>%
  mutate(poi_county = as.numeric(substr(poi_cbg, 1, 6))) %>% # Add county indicator
  filter(poi_cbg >= 370000000000 & poi_cbg < 380000000000) # Filter to just CBGs in NC

# Inspect NA values in the dataset:
summary(is.na(dat))

# It is okay if there are NAs for parent_placekey (not every POI has a parent place), 
# raw_visit_counts and raw_visitor_counts (not every POI had visit(or)s), and 
# top_category or sub_category because not every POI has these
# Every POI needs a cbg and a naics_code, and should be in a poi_category. 

# Replace NAs indicating no visits or visitors with a value of 0:
dat$raw_visit_counts[is.na(dat$raw_visit_counts)] <- 0
dat$raw_visitor_counts[is.na(dat$raw_visitor_counts)] <- 0

# Read in the heatwave dataset and filter the dates to align with the mobility data. 
# The heatwave data are at the daily time series, while the mobility data are at the
# weekly time scale. In this code we are aggregating to get the number of each kind of 
# heatwave that occurred during each week to match with the mobility data. 

# EHF heatwave metrics: 
ehf <- read_parquet("Data/Temp/Processed/CBG_2010/EHF_Heatwave_NC2010CBG_2012_2024.parquet")%>%
  filter(Date >= "2021-12-27" & Date <= "2024-07-31")%>% # Filter dates to match mobility data
  mutate(poi_cbg=as.numeric(cbg))%>% # Duplicate and rename cbg column to match mobility data for the join
  mutate(date_range_start = floor_date(Date - days(1), "week") + days(1)) %>% # Create weekly date variable to match mobility dataset
  group_by(poi_cbg, date_range_start) %>%
  summarize(across(c(Heatwave, low_intensity, moderate_intensity, high_intensity, Severe_Heatwaves, Extreme_Heatwaves), 
                   \(x) sum(x, na.rm = TRUE), 
                   .names = "{.col}")) %>%
  rename(EHF_HW = Heatwave, 
         EHF_low = low_intensity, 
         EHF_mod = moderate_intensity,
         EHF_high = high_intensity, 
         EHF_severe = Severe_Heatwaves, 
         EHF_extreme = Extreme_Heatwaves)


# Group the mobility data by date, census block group, and poi_category, and then 
# summarize visits and visitors by naics code

library(data.table)

setDT(dat)  # convert to data.table by reference
dat_grp <- dat[, .(
  raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE),
  raw_visitor_counts = sum(raw_visitor_counts, na.rm = TRUE)
), by = .(date_range_start, poi_cbg, naics_code)]

naics <- dat %>%
  select(naics_code, poi_category, top_category, sub_category) %>%
  distinct(naics_code, .keep_all = TRUE)
  
dat_grp <- dat_grp %>%
  left_join(naics, by = "naics_code")
rm(naics)
  
# Merge with heatwave data 

dat_merge <- dat_grp %>%
  left_join(ehf, by=c("poi_cbg", "date_range_start"))

write_parquet(dat_merge, "Data/Weekly_Series_Visits_EHF_Merge_2022_2024.parquet")

gc()





























