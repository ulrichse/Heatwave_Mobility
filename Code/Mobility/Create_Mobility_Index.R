
library(tidyverse)


# Set location of the Safegraph data. For me this is in a Sharepoint folder that
# I added as a shortcut to my file explorer: 

data_dir <- "C:/Users/ulric/OneDrive - University of North Carolina at Chapel Hill/DIS/Shared Documents - Safegraph_data/advan_sg/weekly_patterns_NC"

# Read in od_daytime files
od_file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_od_daytime_.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
od_dat <- od_file_list %>%
  map_dfr(~read_csv(.x, col_types = cols(.default = col_character())))

# Read in poi_info files
poi_file_list <- list.files(path = data_dir, pattern = "^NC_weeklypatterns_poi_info_.*\\.csv$", full.names = TRUE)

# Combine all files into a single dataframe
poi_dat <- poi_file_list %>%
  map_dfr(read_csv)

# Convert to character so I can use str_starts
poi_dat <- poi_dat %>%
  mutate(naics_code = as.character(naics_code))

library(arrow)

dat723 <- read_parquet(file.choose())


mobility_all <- dat723 %>%
  group_by(cbg, date_range_start) %>%
  summarize(trips_away = sum(total_visits),
            trips_away_norm = sum(total_visits)/pop2019)
