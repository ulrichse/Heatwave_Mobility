
# Load libraries 

library(sf)
library(tigris)
library(tidyverse)
library(tmap)
library(readr)

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

divisions <- st_transform(divisions, crs = (st_crs(cbg_2010)))

# Read in RUCC county codes from USDA ERS

url <- "https://www.ers.usda.gov/data-products/rural-urban-continuum-codesRuralurbancontinuumcodes2023.csv"

library(readr)
url <- "https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/53251/Ruralurbancontinuumcodes2023.csv?v=41737"
df <- read_csv(url)

df <- df %>%
  filter(State == "NC") %>%
  pivot_wider(
    id_cols = c(FIPS, State, County_Name),       
    names_from = Attribute,                    
    values_from = Value                        
  )%>%
  mutate(
    county = as.numeric(substr(FIPS, 3, 5)))
  
# Execute spatial joins

cbg_2010 <- st_join(cbg_2010, divisions, join = st_intersects, largest = TRUE)

# Join RUCC data

cbg_2010 <- cbg_2010 %>%
  left_join(df, by = 'county')

# Filter for vars of interest

cbg_2010 <- cbg_2010 %>%
  select(cbg, CLIMDIV, CD_NEW, NAME, RUCC_2023)

# Also do for 2020 CBG boundaries

cbg_2020 <- st_join(cbg_2020, divisions, join = st_intersects, largest = TRUE)

# Join RUCC data

cbg_2020 <- cbg_2020 %>%
  left_join(df, by = 'county')

# Filter for vars of interest

cbg_2020 <- cbg_2020 %>%
  select(cbg, CLIMDIV, CD_NEW, NAME, RUCC_2023)

tm_shape(cbg_2020) + 
  tm_polygons(fill = "RUCC_2023")

