
# Code used to create M6_Hourly_Visits_Categorized_2022_2024.parquet, which takes 
# aggregates POI visits based on trip purpose, CBG, and date.

## LOAD LIBRARIES ----
library(arrow)
library(tidyverse)
library(data.table)

setwd("C:/Users/ulric/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility/")

## READ IN HOURLY POI VISITS ----
dat <- read_parquet("Data/Mobility/M5_Hourly_POI_Visits_2022_2024.parquet")

## NAICS CODE LIST ----
naics_code <- dat %>%
  distinct(naics_code, top_category, sub_category)
# There are ~401 unique NAICS code included in this dataset
unique(naics_code$top_category)
# There are 197 unique top categories
unique(naics_code$sub_category)
# There are 351 unique sub categories

## CATEGORIZE NAICS CODES ----

#### Retail ----
retail <- c(4431,
            4441,
            4442,
            4481,
            441110,
            441120,
            441228,
            441310,
            442110,
            443130,
            444220,
            446110,
            446130,
            446191,
            448110,
            448120,
            448140,
            448310,
            448320,
            451110,
            451120,
            451140,
            452210,
            452319,
            453110,
            453210,
            453220,
            453310,
            453920,
            453991,
            453998,
            812112,
            812199
)

#### Food ----
# Food-based retail includes grocery stores and restauraunts
food <- c(445110,
          445120,
          445210,
          445230,
          445292,
          445310,
          722410,
          722511,
          722513,
          722515
)

#### Recreation ----
# Indoor recreation and entertainment activities 
indoor_rec <- c(
  712120,
  712130,
  712190
)

# Outdoor recreation and entertainment
recreation <- c(713990,
                7212,
                
                
)

## CREATE poi_type ----
dat <- dat %>%
  mutate(poi_type = ifelse(naics_code %in% retail, "retail",
                           ifelse(naics_code %in% food, "food",
                                  ifelse(naics_code %in% indoor_rec, "indoor_rec",
                                  "undef"))))
table(dat$poi_type)

## GROUP BASED ON poi_type ----
dat_grp <- dat %>%
  filter(poi_type != "undef") 

# This grouping operation goes so much faster using data.table instead of dplyr stuff
setDT(dat_grp)

cols_sum <- grep("^(DAY|raw)", names(dat_grp), value = TRUE)

dat_grp <- dat_grp[
  ,
  lapply(.SD, sum, na.rm = TRUE),
  by = .(date_range_start, poi_cbg, poi_type),
  .SDcols = cols_sum
]

## WRITE FILE ----
write_parquet(dat_grp, "Data/Mobility/M6_Hourly_Visits_Categorized_2022_2024.parquet")

rm(dat)
rm(dat_grp)
rm(naics_code)
