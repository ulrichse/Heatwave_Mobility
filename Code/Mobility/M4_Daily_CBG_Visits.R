

### Code used to create M4_Daily_CBG_Visits_Expected.parquet, which takes weekly POI visitation 
### and aggregates visits to the census block group level, and then uses rolling means to calculate
### expected visits adjusted for yearly, monthly, and weekly trends. 

### SETUP ----
library(tidyverse)
library(arrow)

### Set location of the Advan/SafeGraph data. For me this is in a Sharepoint folder that
### I added as a shortcut to my file explorer: 

### Use the first one for my desktop and the second one for my laptop:
#data_dir <- "~/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC"
data_dir <- "C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC" 

## LIST OF WEELY FILES WITH DAILY VISITS 
file_list <- list.files(
  path = data_dir,
  pattern = "^NC_weeklypatterns_daily_.*\\.csv$",
  full.names = TRUE
)

## BIND WEEKLY FILES ----
dt_list <- lapply(file_list, fread,
                  select = c("placekey", "date_range_start",
                             "DAY1","DAY2","DAY3","DAY4","DAY5","DAY6","DAY7"))

daily_dat <- rbindlist(dt_list)

## PIVOT TO DAILY ----
daily_long <- melt(
  daily_dat,
  id.vars = c("placekey", "date_range_start"),
  variable.name = "day",
  value.name = "visitor_count"
)

daily_long[, day_num := as.integer(sub("DAY", "", day))]
daily_long[, date := as.IDate(date_range_start) + (day_num - 1)]

daily_long <- daily_long[, .(placekey, date, visitor_count)]

daily_long <- unique(daily_long, by = c("placekey", "date"))

gc()
## JOIN POI INFO ----
# Read in POI info plus and set as a data table
poi_dat <- open_dataset("Data/Mobility/POI_Info_Plus_2022_2024.parquet")

setDT(poi_dat)

poi_dat <- poi_dat[
  , .(
    naics_code = first(naics_code),
    poi_cbg    = first(poi_cbg)
  ),
  by = placekey
]

daily_long <- poi_dat[daily_long, on = "placekey"]

rm(poi_dat)
gc()

 ## GROUP VISITS BY CBG ----

dat <- daily_long[
  , .(visits = sum(visitor_count, na.rm = TRUE)),
  by = .(date, poi_cbg)
]

gc()

## CALCULATE EXPECTED VISITS ----
# Rolling mean over year
dat[, ma_year := frollmean(
  visits,
  n = 365,
  align = "center",
  na.rm = TRUE,
  fill = NA   # fill edges with NA where window can't be centered
),
by = poi_cbg]

# Fill edges with left- or right-aligned rolling mean if NA remains
# Not all dates have enough prior or lag dates for a centered mean
dat[, ma_year := fcoalesce(
  ma_year,
  frollmean(visits, n = 365, align = "left", na.rm = TRUE, fill = NA),
  frollmean(visits, n = 365, align = "right", na.rm = TRUE, fill = NA)
), by = poi_cbg]

# Rolling mean over month (30-day period)
dat[, ma_month := frollmean(
  visits,
  n = 30,
  align = "center",
  na.rm = TRUE
),
by = poi_cbg]

# Left or right align for NAs
dat[, ma_month := fcoalesce(
  ma_month,
  frollmean(visits, n = 30, align = "left", na.rm = TRUE, fill = NA),
  frollmean(visits, n = 30, align = "right", na.rm = TRUE, fill = NA)
), by = poi_cbg]

# Rolling mean over week 
dat[, ma_week := frollmean(
  visits,
  n = 7,
  align = "center",
  na.rm = TRUE
),
by = poi_cbg]

dat[, ma_week := fcoalesce(
  ma_week,
  frollmean(visits, n = 7, align = "left", na.rm = TRUE, fill = NA),
  frollmean(visits, n = 7, align = "right", na.rm = TRUE, fill = NA)
), by = poi_cbg]

### WRITE FILE

write_parquet(dat, "Data/Mobility/M4_Daily_CBG_Visits_Expected.parquet")



