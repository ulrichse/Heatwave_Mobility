
# Load libraries 

library(sf)
library(tigris)
library(tidyverse)
library(tmap)
library(readr)
library(tidycensus)
library(readxl)

# Read in NC climate division shapefile

url <- "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/CONUS_CLIMATE_DIVISIONS.shp.zip"
tmp <- tempfile(fileext = ".zip")

download.file(url, tmp, mode = "wb")
unzip(tmp, exdir = tempdir())

shp_path <- list.files(tempdir(), pattern = "\\.shp$", full.names = TRUE)

climdiv <- st_read(shp_path)%>%
  filter(STATE_FIPS == "37")

# Read in census block group boundaries for 2010 and 2020

# 2010
cbg_2010 <- block_groups(state = "NC", year = "2010") %>%
  mutate(cbg=as.numeric(GEOID10))%>%
  mutate(county=as.numeric(COUNTYFP))%>%
  dplyr::select(cbg, county, geometry)

# 2020
cbg_2020 <- block_groups(state = "NC", year = "2020") %>%
  mutate(cbg=as.numeric(GEOID))%>%
  mutate(county=as.numeric(COUNTYFP))%>%
  dplyr::select(cbg, county, geometry)

# CRS reproject

climdiv <- st_transform(climdiv, crs = (st_crs(cbg_2010)))

# Read in RUCC county codes from USDA ERS

ruca <- read_csv("Data/RUCA/ruca-codes-2020-tract.csv") %>%
  filter(StateName20=="North Carolina") %>%
  select(TractFIPS23, TractFIPS20, PrimaryRUCA, SecondaryRUCA)

cw <- read_xlsx("Data/Crosswalk/CENSUS_TRACT_CROSSWALK_2010_to_2020_2020.xlsx") %>%
  rename(TractFIPS20 = GEOID_2020)

ruca <- ruca %>%
  left_join(cw, by = "TractFIPS20") %>%
  distinct(GEOID_2010, .keep_all = TRUE) %>%
  mutate(tract = as.numeric(GEOID_2010)) %>%
  select(tract, PrimaryRUCA, SecondaryRUCA)  
rm(cw)

# Execute spatial joins
cbg_2010 <- st_join(cbg_2010, climdiv, join = st_intersects, largest = TRUE)

# Join RUCC data

cbg_2010 <- cbg_2010 %>%
  mutate(tract = as.numeric(substr(cbg, 1, 11))) %>%
  left_join(ruca, by = 'tract') 

cbg_2010 <- cbg_2010 %>%
  mutate(RUCC_Cat = ifelse(PrimaryRUCA < 4, "Metro",
                           ifelse(PrimaryRUCA >= 4 &PrimaryRUCA <= 7, "Suburban",
                                  ifelse(PrimaryRUCA >= 8 & PrimaryRUCA <= 9, "Rural",
                                                "Undefined"))))

# Add population count

pop_data <- get_acs(
  geography = "block group",
  variables = "B01003_001",  # Total population
  state = "NC",
  year = 2019,
  survey = "acs5",
  output = "wide"  # Gives estimate and margin of error in columns
)%>%
  mutate(pop2019 = B01003_001E,
         cbg = as.numeric(GEOID))%>%
  select(cbg, pop2019)

cbg_2010 <- cbg_2010 %>%
  left_join(pop_data, by=c('cbg'))

# Physiographic region
cbg_2010 <- cbg_2010 %>%
  mutate(phys_region = ifelse(CD_NEW < 3, "Mountains",
                              ifelse(CD_NEW >= 3 & CD_NEW <= 5, "Piedmont", 
                                     ifelse(CD_NEW > 5, "Coast",
                                            "Undefined"))))

# Filter for vars of interest

cbg_2010 <- cbg_2010 %>%
  select(cbg, pop2019, CLIMDIV, CD_NEW, NAME, PrimaryRUCA, RUCC_Cat, phys_region) %>%
  st_drop_geometry()

write.csv(cbg_2010, "Data/CBG2010_Database.csv")


