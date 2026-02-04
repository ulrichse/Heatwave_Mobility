
## SETUP ----
library(arrow)
library(tidyverse)
library(data.table)
library(lubridate)

setwd("C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility/")

## READ IN HOURLY TRIPS BY CATEGORY ----
dat <- read_parquet("Data/Mobility/M5_Hourly_POI_Visits_2022_2024.parquet")

## PIVOT TO DAILY TIME SERIES ----
# Set 'dat' as a data table
setDT(dat)

# Identify columns that begin with "DAY"
cols_day <- grep("^DAY", names(dat), value = TRUE)

# Melt data into long format
dat <- melt(
  dat,
  id.vars = c("placekey", "date_range_start", "poi_cbg", "raw_visitor_counts", "raw_visit_counts"), # keep other id columns
  measure.vars = cols_day,
  variable.name = "day_hour",
  value.name = "visits"
)

# Split DAYx_HOURy_HOURz into dow and hour_block
dat[, c("dow", "start_block") := tstrsplit(day_hour, "_", fixed = TRUE, keep = c(1,2))]
dat[, dow := as.integer(sub("DAY", "", dow))]

# Compute date
dat[, Date := date_range_start + (dow - 1)]

gc()

## DETRENDING ----
# Need to account for day-of-week effects, monthly patterns within May-Sept, 
# long term trends such as overall growth or decline in POI visits from 2022 to 
# 2024, as well as additional exogenous effects 

### Visualize overall trends across all POIs----
# Aggregate daily visits by poi_type
dat_fac <- dat[, .(daily_visits = sum(visits, na.rm = TRUE)), by = .(Date, placekey)]

ggplot(dat_fac, aes(x = Date, y = daily_visits)) +
  geom_line(color = "steelblue") +
  facet_wrap(~ poi_type, scales = "free_y") +
  labs(title = "Daily Visits by POI Type (Warm Months)", x = "Date", y = "Visits") +
  theme_minimal()

# Average visits per day-of-week and poi_type
dow_fac <- dat[, .(avg_visits = mean(visits, na.rm = TRUE)), by = .(dow, poi_type)]

ggplot(dow_fac, aes(x = dow, y = avg_visits)) +
  geom_col(fill = "skyblue") +
  facet_wrap(~ poi_type, scales = "free_y") +
  labs(title = "Average Visits by Day of Week by POI Type (Warm Months)", x = "Day of Week", y = "Visits") +
  theme_minimal()

# Average visits per month and poi_type
month_fac <- dat[, .(monthly_visits = sum(visits, na.rm = TRUE)), by = .(month, poi_type)]

ggplot(month_fac, aes(x = month, y = monthly_visits)) +
  geom_col(fill = "orange") +
  facet_wrap(~ poi_type, scales = "free_y") +
  labs(title = "(Warm) Monthly Visits by POI Type", x = "Month", y = "Total Visits") +
  theme_minimal()

# Based on these graphs, we must adjust for year, month, and day of week!

## ADD TIME FEATURES ----
# dow is already present
dat[, year := year(Date)]
dat[, month := month(Date)]

## DETREND USING RESIDUALS ----

# Detrending using residuals from a linear model
# Model the expected visits based on year, month, and day-of-week
# and then take the residuals as the detrended visits. 

# Remove year effects
dat[, visits := visits - mean(visits), by = .(placekey, year)]

# Remove month effects
dat[, visits := visits - mean(visits), by = .(placekey, month)]

# Remove day-of-week effects
dat[, visits_detrended := visits - mean(visits), by = .(placekey, dow)]

write_parquet(dat, "Data/Mobility/M6_Hourly_POI_Visits_Adjusted.parquet")







