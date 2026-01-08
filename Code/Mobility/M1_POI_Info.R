
# Code to create POI categories using NAICS codes 
# OUTPUT FILE: POI_Info_Plus_2022_2024.parquet

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

# Categorize NAICS codes
# Create broad categories of NAICS codes, based on https://www.census.gov/programs-surveys/economic-census/year/2022/guidance/understanding-naics.html
# Sector & Description
# 11	Agriculture, Forestry, Fishing and Hunting
# 21	Mining, Quarrying, and Oil and Gas Extraction
# 22	Utilities
# 23	Construction
# 31-33	Manufacturing
# 42	Wholesale Trade
# 44-45	Retail Trade
## 445 Food and Beverage Retailers
# 48-49	Transportation and Warehousing
## 487 Scenic and Sightseeing Transportation
# 51	Information
# 52	Finance and Insurance
# 53	Real Estate and Rental and Leasing
# 54	Professional, Scientific, and Technical Services
# 55	Management of Companies and Enterprises
# 56	Administrative and Support and Waste Management and Remediation Services
# 61	Educational Services
# 62	Health Care and Social Assistance
## 6242 Community Food and Housing, and Emergency and Other Relief Services
## 6244 Child Care Services
# 71	Arts, Entertainment, and Recreation
# 72	Accommodation and Food Services
## 721 Accommodation
## 7224 Drinking Places 
## 7225 Restaurants and Other Eating Places
# 81	Other Services 
## 813 Religious, Grantmaking, Civic, Professional, and Similar Organizations
## 814 Private Households
# 92	Public Administration 

# Read in poi_info files
poi_file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_poi_info_plus_.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
poi_dat <- poi_file_list %>%
  map_dfr(read_csv)

# Write off merged POI info file
write_parquet(poi_dat, "POI_Info_Plus_2022_2024.parquet")

# Below is my draft code for making anchor/discretionary/essential categories: ----

# Code for processing types of POIs: 

naics_codes <- read_csv("Data/2022_NAICS_Structure.csv") 

naics_codes$naics_code <- naics_codes$`2022 NAICS Code` 
naics_codes$code_title <- naics_codes$`2022 NAICS Title` 

naics_codes <- naics_codes %>%
  select(naics_code, code_title)

# Convert to character so I can use str_starts
poi_dat <- poi_dat %>%
  mutate(naics_code = as.character(naics_code))

poi_dat <- poi_dat %>%
  left_join(naics_codes, by = 'naics_code')

poi_dat <- poi_dat %>%
  mutate(destination_purpose = case_when(
    str_starts(poi_category, "Construction") ~ "Anchor",
    str_starts(poi_category, "Manufacturing") ~ "Anchor",
    str_starts(poi_category, "Agriculture, Forestry, Fishing and Hunting") ~ "Anchor",
    str_starts(poi_category, "Mining, Quarrying, and Oil and Gas Extraction") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor",
    str_starts(poi_category, "Wholesale Trade  ") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor"
    
  ))




