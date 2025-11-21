
# Set working directory
setwd("~/Heatwave_Mobility")

# Code to identify EHF heatwave days from 2022 to 2024 at the tract level in NC
library(tidyverse)
library(arrow)
library(data.table)


# Note on Nov 5th, I did this replacing instances of the 85th percentile with instances of the 90th....

# Read in PRISM daily temperature file
Temperature <- read_parquet("Data/Temp/Processed/CBG_2010/PRISM_Dailys_2012_2024_CBG2010.parquet") %>% 
  mutate(month = month(Date)) %>%
  collect()

# EHF Calculations ----
# Calculate warm season (May-Sept) percentiles for each census block group
Warm_Season_Thresholds <- Temperature %>%
  filter(month %in% c(5, 6, 7, 8, 9)) %>%
  group_by(cbg) %>%
  summarize(
    H95 = quantile(tmean, probs = 0.95, na.rm = TRUE),
    H97 = quantile(tmean, probs = 0.97, na.rm = TRUE),
    H99 = quantile(tmean, probs = 0.99, na.rm = TRUE),
    .groups = "drop"
  )

# Join warm season thresholds back to original daily data
Temperature <- Temperature %>%
  left_join(Warm_Season_Thresholds, by = c("cbg")) %>%
  arrange(cbg, Date) %>%
  # Add rolling averages (aligned right = uses current day + previous days)
  group_by(cbg) %>%
  mutate(
    tmean_3day_avg  = rollapply(tmean,  width = 3,  FUN = mean, align = "right", fill = NA),
    tmean_30day_avg = rollapply(tmean, width = 30, FUN = mean, align = "right", fill = NA)
  ) %>%
  ungroup()
rm(Warm_Season_Thresholds)

Temperature <- Temperature %>%
  mutate(EHI_sig = tmean_3day_avg - H95,# Indicates whether the three day period is warm relative to local warm season climate
         EHI_accl = tmean_3day_avg - tmean_30day_avg, # Indicates if three day period is warmer than the prior 30 day average
         EHI_sig = if_else(EHI_sig < 0, 0, EHI_sig), # If EHI_sig is negative it receives a value of 0
         EHI_accl = if_else(EHI_accl < 1, 1, EHI_accl), 
         EHF = EHI_sig * EHI_accl) 

# Calculate 85th percentile EHF

Temperature_1 <- Temperature %>%
  drop_na(EHF) %>%
  group_by(cbg) %>%
  summarise(EHF85 = quantile(EHF[EHF > 0], probs = 0.85, na.rm = TRUE)) %>%
  ungroup()

Temperature <- Temperature %>%
  left_join(Temperature_1, by = "cbg")
rm(Temperature_1)

Temperature_1 <- Temperature %>%
  group_by(cbg) %>%
  mutate(EHF_severe = (ifelse(EHF >= EHF85, 1, 0)),
         EHF_extreme = (ifelse(EHF >= (3 * EHF85), 1, 0)),
         EHF_low = (ifelse(EHF > 0 & EHF < 1, 1, 0)),
         EHF_moderate = (ifelse(EHF >= 1 & EHF < 2, 1, 0)),  
         EHF_high = (ifelse(EHF >= 2, 1, 0)),
         tmean_above_95th = fifelse(tmean > H95, 1, 0),
         tmean_above_97th  = fifelse(tmean > H97, 1, 0),
         tmean_above_99th = fifelse(tmean > H99, 1, 0),
         EHF_any = if_else(EHF_low > 0 | EHF_moderate > 0 | EHF_high > 0, 1, 0)) %>%
  ungroup()

# Percentile-Threshold Calculations ----

# CLIMDIV JOIN -----------------------------------------------------------------
# In this section we will create a census block group database indicating the NOAA 
# climate division that each CBG belongs to. The relative percentile temperature 
# thresholds used to calculate heatwaves are derived at the climate division level. 

# File listing CBGs and corresponding CONUS Climate Division

url <- "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/CONUS_CLIMATE_DIVISIONS.shp.zip"
tmp <- tempfile(fileext = ".zip")

download.file(url, tmp, mode = "wb")
unzip(tmp, exdir = tempdir())

shp_path <- list.files(tempdir(), pattern = "\\.shp$", full.names = TRUE)
shp_path <- "C:\\Users\\ulrichs\\AppData\\Local\\Temp\\RtmpOKgWQV/GIS.OFFICIAL_CLIM_DIVISIONS.shp"

climdiv <- st_read(shp_path)%>%
  filter(STATE_FIPS == "37")

# Read in census block group boundaries: 
cbg_2010 <- block_groups(state = "NC", year = "2010") %>%
  mutate(cbg=as.numeric(GEOID10))%>%
  dplyr::select(cbg, geometry)

climdiv <- st_transform(climdiv, crs = (st_crs(cbg_2010))) # Reproject climate divisions dataset

# Execute spatial joins and filter vars
cbg_2010 <- st_join(cbg_2010, climdiv, join = st_intersects, largest = TRUE)%>%
  select(cbg, CLIMDIV, CD_NEW, NAME)%>%
  st_drop_geometry()

# Read the daily mean, max, and average temperature time series from PRISM
# and merge with climate region file

Temperature <- Temperature %>%
  left_join(cbg_2010, by=c('cbg')) 

# Calculate heatwaves over the warm season (May to June)
calculate_heatwave <- function(df, region_column, temperature_columns, percentiles = c(0.90, 0.95, 0.99)) {
  
  df <- df %>% # Filter for the months of interest
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

df_heatwave <- calculate_heatwave(Temperature, region_column = "CLIMDIV", temperature_column = c("tmin", "tmean", "tmax", "heatindex"), percentiles = c(0.90, 0.95, 0.99))

# Calculate bivariate events
df_heatwave <- df_heatwave %>%
  mutate(above_tmin_tmax_90=ifelse((above_tmin_pct_90==1 & above_tmax_pct_90==1), 1, 0),
         above_tmin_tmax_95=ifelse((above_tmin_pct_95==1 & above_tmax_pct_95==1), 1, 0),
         above_tmin_tmax_99=ifelse((above_tmin_pct_99==1 & above_tmax_pct_99==1), 1, 0))

percentiles <- c("90", "95", "99")

for (p in percentiles) {
  df_heatwave <- df_heatwave %>%
    group_by(CLIMDIV) %>%
    arrange(Date) %>%
    mutate(
      across(
        c(starts_with(paste0("above_tmean_pct_", p)), 
          starts_with(paste0("above_tmin_pct_", p)), 
          starts_with(paste0("above_tmax_pct_", p)),
          starts_with(paste0("above_tmin_tmax_", p)),
          starts_with(paste0("above_heatindex_pct",p))),
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

# Write to .parquet ----

write_parquet(df_heatwave, "Data/Temp/Processed/CBG_2010/HW_NC2010CBG_2012_2024.parquet") # Repeat for 2010




