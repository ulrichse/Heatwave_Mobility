
# Code to create POI categories using NAICS codes 
# Merge with weekly summary data 
# INPUT: Safegraph data files
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

data_dir <- "~/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC/"

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
poi_file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_poi_info_.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
poi_dat <- poi_file_list %>%
  map_dfr(read_csv)

# Convert to character so I can use str_starts
poi_dat <- poi_dat %>%
  mutate(naics_code = as.character(naics_code))

poi_dat <- poi_dat %>%
  mutate(poi_category = case_when(
    str_starts(naics_code, "11") ~ "Agriculture, Forestry, Fishing and Hunting",
    str_starts(naics_code, "21") ~ "Mining, Quarrying, and Oil and Gas Extraction",
    str_starts(naics_code, "22") ~ "Utilities",
    str_starts(naics_code, "23") ~ "Construction",
    str_starts(naics_code, "31") ~ "Manufacturing",
    str_starts(naics_code, "32") ~ "Manufacturing",
    str_starts(naics_code, "33") ~ "Manufacturing",
    str_starts(naics_code, "42") ~ "Wholesale Trade",
    str_starts(naics_code, "44") ~ "Retail Trade",
    str_starts(naics_code, "45") ~ "Retail Trade",
    str_starts(naics_code, "445") ~ "Food and Beverage Retailers",
    str_starts(naics_code, "48") ~ "Transportation and Warehousing",
    str_starts(naics_code, "49") ~ "Transportation and Warehousing",
    str_starts(naics_code, "487") ~ "Scenic and Sightseeing Transportation",
    str_starts(naics_code, "51") ~ "Information",
    str_starts(naics_code, "52") ~ "Finance and Insurance",
    str_starts(naics_code, "53") ~ "Real Estate and Rental and Leasing",
    str_starts(naics_code, "54") ~ "Professional, Scientific, and Technical Services",
    str_starts(naics_code, "55") ~ "Management of Companies and Enterprises",
    str_starts(naics_code, "56") ~ "Administrative and Support and Waste Management and Remediation Services",
    str_starts(naics_code, "61") ~ "Educational Services",
    str_starts(naics_code, "62") ~ "Health Care and Social Assistance",
    str_starts(naics_code, "6242") ~ "Community Food and Housing, and Emergency and Other Relief Services",
    str_starts(naics_code, "6244") ~ "Child Care Services",
    str_starts(naics_code, "71") ~ "Arts, Entertainment, and Recreation",
    str_starts(naics_code, "72") ~ "Accommodation and Food Services",
    str_starts(naics_code, "721") ~ "Accommodation",
    str_starts(naics_code, "7224") ~ "Drinking Places",
    str_starts(naics_code, "7225") ~ "Restaurants and Other Eating Places",
    str_starts(naics_code, "81") ~ "Other Services",
    str_starts(naics_code, "813") ~ "Religious, Grantmaking, Civic, Professional, and Similar Organizations",
    str_starts(naics_code, "814") ~ "Private Households",
    str_starts(naics_code, "92") ~ "Public Administration",
    TRUE ~ "Undefined"
  ))

poi_dat <- poi_dat %>%
  mutate(poi_category = case_when(
    str_starts(naics_code, "445") ~ "Food and Beverage Retailers",
    str_starts(naics_code, "487") ~ "Scenic and Sightseeing Transportation",
    str_starts(naics_code, "6242") ~ "Community Food and Housing, and Emergency and Other Relief Services",
    str_starts(naics_code, "6244") ~ "Child Care Services",
    str_starts(naics_code, "721") ~ "Accommodation",
    str_starts(naics_code, "7224") ~ "Drinking Places",
    str_starts(naics_code, "7225") ~ "Restaurants and Other Eating Places",
    str_starts(naics_code, "813") ~ "Religious, Grantmaking, Civic, Professional, and Similar Organizations",
    str_starts(naics_code, "814") ~ "Private Households",
    TRUE ~ poi_category
  ))

poi_dat <- poi_dat %>%
  mutate(top_category = case_when(
    str_starts(poi_category, "Construction") ~ "Anchor",
    str_starts(poi_category, "Manufacturing") ~ "Anchor",
    str_starts(poi_category, "Agriculture, Forestry, Fishing and Hunting") ~ "Anchor",
    str_starts(poi_category, "Mining, Quarrying, and Oil and Gas Extraction") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor",
    str_starts(poi_category, "Wholesale Trade  ") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor",
    
  ))
# QC, check for undefined values
table(poi_dat$poi_category)

# Read in weekly visitation patterns 
file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_summary.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
dat <- file_list %>%
  map_dfr(read_csv)

# Filter dataframe to avoid many-to-many join
poi_dat <- poi_dat %>%
  distinct(placekey, naics_code, poi_category, top_category, sub_category)

# Merge poi categories with weekly summary data
dat <- dat %>%
  left_join(poi_dat, by=c("placekey"))

# QC
table(is.na(dat$poi_category))

write_parquet(dat, "Data/Mobility/Weekly_Patterns_POI_2022_2024.parquet")




