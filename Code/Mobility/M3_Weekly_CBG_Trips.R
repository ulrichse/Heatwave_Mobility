
# Code to process OD Daytime files

library(arrow)
library(tidyverse)
library(data.table)

#setwd("Heatwave_Mobility/")

# Set location of the Safegraph data. For me this is in a Sharepoint folder that
# I added as a shortcut to my file explorer: 

data_dir <- "~/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC"

# Read in distance matrix and CBG database: 

dist_df <- read_parquet("Data/CBG_Dist_Matrix.parquet")

cbg_dbase <- read_csv("Data/CBG2010_Database.csv") %>% select(-...1)

# 3) Read in weekly OD daytime files ----

# Read in and filter POI info; this provides the destination CBG
poi_dat <- read_parquet("Data/Mobility/POI_Info_Plus_2022_2024.parquet") %>%
  select(placekey, poi_cbg) %>%
  distinct(placekey, poi_cbg)

# Read in od_daytime files, merge into single file, join POI CBG identifier

files <- list.files(
  path = data_dir,
  pattern = "^NC_weeklypatterns_od_home_.*\\.csv$",
  full.names = TRUE
)

od_dat <- rbindlist(
  lapply(files, fread,
         select = c("placekey", "visitor_home_cbgs", "poi_cbg", "count", "date_range_start"),
         showProgress = FALSE),
  use.names = TRUE
)

od_dat$origin <- as.numeric(od_dat$visitor_home_cbgs)

od_dat <- od_dat %>%
left_join(poi_dat, by = "placekey")

od_dat <- od_dat %>%
 filter(substr(visitor_home_cbgs, 1, 2) == "37") 

od_dat_grp <- od_dat %>%
  group_by(origin, poi_cbg, date_range_start) %>%
  summarize(unique_pois = n_distinct(placekey), 
           total_visits = sum(count), 
           .groups = "drop") 
rm(od_dat)
gc()

dist_df$origin = as.numeric(dist_df$origin)
dist_df$poi_cbg = as.numeric(dist_df$destination)

od_dat_grp <- od_dat_grp %>%
  select(origin, poi_cbg, date_range_start, unique_pois, total_visits)
  left_join(dist_df, by = c("origin", "poi_cbg")) 
summary(is.na(od_dat_grp))

write_parquet(od_dat_grp, "Data/Mobility/od_dat_grp.parquet")
%>%

  mutate(distance_miles = as.numeric(distance_miles)) %>%
  mutate(dist_cat = case_when(
      distance_miles < 1 ~ "dist_under1mi",
      distance_miles < 5 ~ "dist_5to25",
      distance_miles < 25 ~ "dist_25to50",
      distance_miles < 50 ~ "dist_25to50",
      distance_miles >= 50 ~ "dist_above50",
      TRUE ~ "undefined"
    ))

library(data.table)

setDT(od_dat)
setDT(dist_df)

od_dat_grp <- od_dat[
  substr(visitor_home_cbgs, 1, 2) == "37",
  .(
    unique_pois  = uniqueN(placekey),
    total_visits = sum(count)
  ),
  by = .(origin = as.integer(visitor_home_cbgs),
         poi_cbg,
         date_range_start)
][
  dist_df,
  on = .(origin, poi_cbg = destination)
][
  , dist_cat := fifelse(distance_miles < 1, "dist_under1mi",
                        fifelse(distance_miles < 5, "dist_5to25",
                                fifelse(distance_miles < 25, "dist_25to50",
                                        fifelse(distance_miles < 50, "dist_25to50",
                                                "dist_above50"))))
]


  
od_dat_grp <- od_dat_grp %>%
  group_by(visitor_home_cbgs, dist_cat, date_range_start) %>%
  summarize(unique_pois = sum(unique_pois), total_visits = sum(total_visits), .groups = "drop") %>%
  mutate(poi_visit_index = unique_pois / total_visits,
           visit_poi_index = total_visits / unique_pois,
           cbg = as.numeric(visitor_home_cbgs)) %>%
  left_join(cbg_dbase, by = "cbg") %>%
  filter(pop2019 > 0) %>%
  mutate(visits_pop = total_visits / pop2019)
  
# Save final dataset
write_parquet(od_dat_grp, 


