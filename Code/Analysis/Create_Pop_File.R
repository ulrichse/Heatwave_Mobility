
library(tidyverse)
setwd("C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility")

# Construct population file ----

library(tidycensus)

get_pop <- function(years) {
  map_dfr(years, ~ {
    get_acs(
      geography = "block group",
      variables = c(pop = "B01003_001"),
      state = "NC",
      survey = "acs5",
      year = .x,
      geometry = FALSE
    ) %>%
      transmute(
        cbg  = as.numeric(GEOID),
        year = .x,
        population = estimate
      )
  })
}

# Pre 2020 pop because CBG boundaries change in 2020
pre2020 <- get_pop(2013:2019)

# Apply 2013 population to 2012
pre2020 <- bind_rows(
  pre2020,
  pre2020 %>% filter(year == 2013) %>% mutate(year = 2012)
) %>%
  arrange(cbg, year)

# Post 2020 population 
post2020 <- get_pop(2020:2022)

# Apply 2022 population to 2023, 2024
extra_years <- 2023:2024
post2020 <- bind_rows(
  post2020,
  map_dfr(extra_years, ~ post2020 %>% filter(year == 2022) %>% mutate(year = .x))
) %>%
  arrange(cbg, year)

# Crosswalk file for 2020 to 2010

cw <- read.csv("Data/Crosswalk/nhgis_bg2020_bg2010_37.csv") %>%
  select(bg2020ge, bg2010ge, wt_pop) %>%
  mutate(cbg = bg2010ge, 
         cbg20 = bg2020ge)

post2020 <- post2020 %>%
  left_join(cw, by = c("cbg" = "cbg20")) %>%
  mutate(pop_weighted = population * wt_pop)

post2020 <- post2020 %>%
  dplyr::summarize(population = sum(pop_weighted, na.rm = TRUE), .by = c(bg2010ge,year)) %>%
  rename(cbg=bg2010ge)

full_pop <- bind_rows(pre2020, post2020) %>%
  arrange(cbg, year)

write.csv(full_pop, "Data/Pop/Annual_ACS_Pop_2012_2024_2010CBG.csv")
