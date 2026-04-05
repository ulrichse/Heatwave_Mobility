
### SETUP: ----
### Load libraries: 
library(tidyverse)
library(arrow)
library(data.table)
library(zoo)
library(sf)
library(tigris)
library(lubridate)

### PART ONE: IDENTIFY HEATWAVES: ----
#### LOAD PRISM DAILY METRICS: ----
Temperature <- read_parquet("data/processed/PRISM_Dailys_1981_2025_CBG2020.parquet")

#### CALCULATE RELATIVE-PERCENTILE HEATWAVE DAYS: ----

#### Function to identify days above meteorological thresholds (90th, 95th, and 99th percentiles)
#### during the warm season (May to Sept):
calculate_heatwave <- function(df, temperature_columns, percentiles = c(0.90, 0.95, 0.99)) {
 
  ### Filter for the months of interest (May to September) and group by CBG:
  df <- df %>% 
    filter(month(Date) >= 5 & month(Date) <= 9) %>%
    group_by(CBG) ### The percentile threshold is unique for each CBG
  
  ### Iterate the following function over each specified temperature column:
  for (temp_col in temperature_columns) {
    
    ### Identify percentile and create binary indicator variable:
    for (p in percentiles) {
      pct_col_name <- paste0(temp_col, "_pct_", p * 100)
      above_col_name <- paste0("above_", temp_col, "_pct_", p * 100)
      
      ### Assign values: 
      df <- df %>%
        mutate(
          !!pct_col_name := quantile(!!sym(temp_col), p, na.rm = TRUE),
          !!above_col_name := as.numeric(!!sym(temp_col) > !!sym(pct_col_name))
        )
    }
  }
  
  ### Ungroup and return final dataframe:
  df <- df %>% ungroup()
  
  return(df)
}

#### Run the function: 
df_heatwave <- calculate_heatwave(df = Temperature, 
                                  temperature_column = c("tmin", "tmax", "tmean", "heatindex"), 
                                  percentiles = c(0.90, 0.95, 0.99))

### Set as data.table for the remaining calculations: 
setDT(df_heatwave)

#### CALCULATE BIVARIATE HEATWAVE EVENTS: ----
#### These are heatwave events where both a minimum and maximum daily temperature 
#### threshold have to be exceeded.
df_heatwave[, `:=`(
  above_tmin_tmax_pct_90 = as.integer(above_tmin_pct_90 == 1 & above_tmax_pct_90 == 1),
  above_tmin_tmax_pct_95 = as.integer(above_tmin_pct_95 == 1 & above_tmax_pct_95 == 1),
  above_tmin_tmax_pct_99 = as.integer(above_tmin_pct_99 == 1 & above_tmax_pct_99 == 1)
)]

#### CALCULATE EXCESS HEAT FACTOR (EHF) ----
#### EHF involves the application of rolling 3- and 30-day averages:
setorder(df_heatwave, CBG, Date)

df_heatwave[, `:=`(
  tmean_3day_avg  = frollmean(tmean, 3, align = "right", na.rm = TRUE),
  tmean_30day_avg = frollmean(tmean, 30, align = "right", na.rm = TRUE)
), by = CBG]

#### Calculate the value of the EHF: 
### EHI_sig indicates whether the three day period is warm relative to local warm season climate
### EHI_accl indicates if three day period is warmer than the prior 30 day average
### If EHI_sig is negative it receives a value of 0
### pmax(a,b) compares two values and keeps the largest one
df_heatwave[, `:=`(
  EHI_sig  = pmax(tmean_3day_avg - tmean_pct_95, 0),
  EHI_accl = pmax(tmean_3day_avg - tmean_30day_avg, 1)
)]

df_heatwave[, EHF := EHI_sig * EHI_accl]

#### IDENTIFY EHF HEATWAVE DAYS: 
df_heatwave[, `:=`(
  EHF_low      = as.integer(EHF > 0 & EHF < 1),
  EHF_moderate = as.integer(EHF >= 1 & EHF < 2),
  EHF_high     = as.integer(EHF >= 2)
)]

#### Write the clean heatwave file to .parquet 
write_parquet(df_heatwave, "data/clean/NC_CBG2020_Heatwaves.parquet") 

### PART TWO: CONVERT HEAT DATA INTO WEEKLY TIME SERIES: ----
### The mobility data are at the weekly time scale, so the heatwave file needs
### to match for later analysis. 

### Filter to study years:
df_heatwave <- df_heatwave[year(Date) %in% 2022:2025] 
### Create DATE_RANGE_START to match mobility data:  
df_heatwave[, DATE_RANGE_START := as.Date(floor_date(Date-1, "week") + 1)]
### Specify the heatwave columns to group by: 
hw_defs <- grep("^(EHF|above_)", names(df_heatwave), value = TRUE)
### Aggregate heatwaves to weekly counts: 
### .SD means subset of data in data.table language
df_weekly <- df_heatwave[, lapply(.SD, function(x) sum(x, na.rm = TRUE)),
                         by = .(CBG, DATE_RANGE_START),
                         .SDcols = hw_defs]

rm(df_heatwave)

### PART THREE: MERGE HEATWAVES WITH MOBILITY DATA: ----
#### LOAD PRE-PROCESSED WEEKLY ORIGIN-DESTINATION DATA:
mobility_dat <- read_parquet("data/mobility/NC_weeklypatternsplus_weekly_trips_visits_adj.parquet")

#### MERGE WITH HEATWAVE DATAFRAME: 
dat <- mobility_dat %>%
  left_join(df_weekly, by = c("CBG", "DATE_RANGE_START"))

### Filter NA values: 
dat <- dat %>%
  filter(!is.na(TRIP_COUNT)) %>%
  filter(!is.na(above_tmin_pct_90))

#### WRITE CLEANED AND MERGED FILE: 
write_parquet(dat, "data/clean/Heatwave_Mobility_GEOG515.parquet")
