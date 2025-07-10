
# Create 2, 3, and 4-day heatwave events using tmean, tmin and tmax (bivariate/compound) definitions at 75th through 99th percentiles

library(data.table)
library(lubridate)
library(dplyr)
library(arrow)
library(ggpubr)
library(tidyverse)
library(sf)
library(tigris)

setwd("~/Heatwaves_Mobility")

# CLIMDIV JOIN -----------------------------------------------------------------
# In this section we will create a census block group database (for both 2010 and 
# 2020) indicating the NOAA climate division that each CBG belongs to. The relative
# percentile temperature thresholds used to calculate heatwaves are derived at the 
# climate division level 

# File listing CBGs and corresponding CONUS Climate Division

url <- "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/CONUS_CLIMATE_DIVISIONS.shp.zip"
tmp <- tempfile(fileext = ".zip")

download.file(url, tmp, mode = "wb")
unzip(tmp, exdir = tempdir())

shp_path <- list.files(tempdir(), pattern = "\\.shp$", full.names = TRUE)

climdiv <- st_read(shp_path)%>%
  filter(STATE_FIPS == "37")

# Read in census block group boundaries: 

# 2010
cbg_2010 <- block_groups(state = "NC", year = "2010") %>%
  mutate(cbg=as.numeric(GEOID10))%>%
  dplyr::select(cbg, geometry)

# 2020
cbg_2020 <- block_groups(state = "NC", year = "2020") %>%
  mutate(cbg=as.numeric(GEOID))%>%
  dplyr::select(cbg, geometry)

# Reproject climate divisions dataset

climdiv <- st_transform(climdiv, crs = (st_crs(cbg_2010)))

# Execute spatial joins and filter vars

cbg_2010 <- st_join(cbg_2010, climdiv, join = st_intersects, largest = TRUE)%>%
  select(cbg, CLIMDIV, CD_NEW, NAME)%>%
  st_drop_geometry()

cbg_2020 <- st_join(cbg_2020, climdiv, join = st_intersects, largest = TRUE) %>%
  select(cbg, CLIMDIV, CD_NEW, NAME)%>%
  st_drop_geometry()

# Read the daily mean, max, and average temperature time series from PRISM
# and merge with climate region file

# Replace file path and file name with 2020 if you are using 2020 boundaries: 

df <- read_parquet("Data/Temp/Processed/CBG_2010/PRISM_dailys_2012_2024_CBG2010.parquet")%>% # Change here if using 2020
  filter(year(Date) >= 2012 & year(Date) <= 2024)%>%
  mutate(cbg=as.numeric(cbg))%>%
  rename(tmin = Tmin,
         tmax = Tmax, 
         tmean = Tmean)%>%
  left_join(cbg_2010, by=c('cbg')) # CHANGE HERE if using 2020

# Calculate heatwaves over the warm season
calculate_heatwave <- function(df, region_column, temperature_columns, percentiles = c(0.75, 0.85, 0.90, 0.95, 0.975, 0.99)) {
  
  # Filter for the months of interest
  df <- df %>%
    filter(month(Date) >= 5 & month(Date) <= 9) %>%
    group_by(across(all_of(region_column)))
  
  # Iterate over temperature columns
  for (temp_col in temperature_columns) {
    
    # Calculate quantiles for each percentile
    for (p in percentiles) {
      pct_col_name <- paste0(temp_col, "_pct_", p * 100)
      above_col_name <- paste0("above_", temp_col, "_pct_", p * 100)
      
      # Calculate percentile and check if value exceeds it
      df <- df %>%
        mutate(
          !!pct_col_name := quantile(!!sym(temp_col), p, na.rm = TRUE),
          !!above_col_name := as.numeric(!!sym(temp_col) > !!sym(pct_col_name))
        )
    }
  }
  
  # Ungroup and return the final dataframe
  df <- df %>% ungroup()
  
  return(df)
}

# Calculate heatwave and store the result in a new dataframe: 

# 2020 CBG boundaries ----------------------------------------------------------
df_heatwave <- calculate_heatwave(df, region_column = "CLIMDIV", temperature_column = c("tmin", "tmean", "tmax"), percentiles = c(0.75, 0.85, 0.90, 0.95, 0.975, 0.99))

df_heatwave <- df_heatwave %>%
  mutate(above_tmin_tmax_75=ifelse((above_tmin_pct_75==1 & above_tmax_pct_75==1), 1, 0),
         above_tmin_tmax_85=ifelse((above_tmin_pct_85==1 & above_tmax_pct_85==1), 1, 0),
         above_tmin_tmax_90=ifelse((above_tmin_pct_90==1 & above_tmax_pct_90==1), 1, 0),
         above_tmin_tmax_95=ifelse((above_tmin_pct_95==1 & above_tmax_pct_95==1), 1, 0),
         above_tmin_tmax_97.5=ifelse((above_tmin_pct_97.5==1 & above_tmax_pct_97.5==1), 1, 0),
         above_tmin_tmax_99=ifelse((above_tmin_pct_99==1 & above_tmax_pct_99==1), 1, 0))

percentiles <- c("75", "85", "90", "95", "97.5", "99")

for (p in percentiles) {
  df_heatwave <- df_heatwave %>%
    group_by(CLIMDIV) %>%
    arrange(Date) %>%
    mutate(
      across(
        c(starts_with(paste0("above_tmean_pct_", p)), 
          starts_with(paste0("above_tmin_pct_", p)), 
          starts_with(paste0("above_tmax_pct_", p)),
          starts_with(paste0("above_tmin_tmax_", p))),
        list(
          two_days = ~ ifelse(. == 1 & lag(.) == 1 & lead(.) == 0, 1, 0),
          three_days = ~ ifelse(. == 1 & lag(., 2) == 1 & lag(.) == 1 & lead(.) == 0, 1, 0),
          four_days = ~ ifelse(. == 1 & lag(., 3) == 1 & lag(., 2) == 1 & lag(.) == 1 & lead(.) == 0, 1, 0)
        ),
        .names = "{col}_{fn}"
      )
    ) %>%
    ungroup()
}

write_parquet(df_heatwave, "Data/Temp/Processed/CBG_2010/NC_CBG2010_Heatwaves.parquet") # CHANGE HERE if using 2020
