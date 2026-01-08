
library(tidyverse)
library(arrow)
library(readxl)


setwd("C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility")

# Read in datasets ----

# Population file:
full_pop <- read.csv("Data/Pop/Annual_ACS_Pop_2012_2024_2010CBG.csv")

# EHF heatwaves: 
ehf_raw <- read_parquet("Data/Temp/Processed/CBG_2010/EHF_Heatwave_NC2010CBG_2012_2024_fixed.parquet") %>%
  mutate(cbg = as.numeric(cbg)) %>%
  select(Date, cbg, Heatwave, low_intensity, moderate_intensity, high_intensity) %>%
  rename(EHF_low_ex = low_intensity,
         EHF_mod_ex = moderate_intensity, 
         EHF_high_ex = high_intensity, 
         EHF_all = Heatwave) 

gc()

# Bivariate, TMEAN, TMAX, heat index heatwaves: 
hws_raw <- read_parquet("Data/Temp/Processed/CBG_2010/NC_CBG2010_Heatwaves_v3.parquet") %>%
  mutate(
    cbg = as.numeric(cbg),
    year = year(Date),
    # Bivariate heatwave definitions
    biv_90_ex  = if_else(above_tmin_pct_90 == 1 & above_tmax_pct_90 == 1 & 
                        above_tmin_pct_95 == 0 & above_tmax_pct_95 == 0 & 
                        above_tmin_pct_99 == 0 & above_tmax_pct_99 == 0, 1, 0),
    biv_95_ex  = if_else(above_tmin_pct_95 == 1 & above_tmax_pct_95 == 1 & 
                        above_tmin_pct_99 == 0 & above_tmax_pct_99 == 0, 1, 0),
    biv_99_ex  = if_else(above_tmin_pct_99 == 1 & above_tmax_pct_99 == 1, 1, 0),
    tmean_90_ex  = if_else(above_tmean_pct_90 == 1 & above_tmean_pct_95 == 0 & above_tmean_pct_99 == 0, 1, 0),
    tmean_95_ex  = if_else(above_tmean_pct_95 == 1 & above_tmean_pct_99 == 0, 1, 0),
    tmean_99_ex  = if_else(above_tmean_pct_99 == 1, 1, 0),
    tmax_90_ex  = if_else(above_tmax_pct_90 == 1 & above_tmax_pct_95 == 0 & above_tmax_pct_99 == 0, 1, 0),
    tmax_95_ex  = if_else(above_tmax_pct_95 == 1 & above_tmax_pct_99 == 0, 1, 0),
    tmax_99_ex  = if_else(above_tmax_pct_99 == 1, 1, 0),
    heatindex_90_ex  = if_else(above_heatindex_pct_90 == 1 & above_heatindex_pct_95 == 0 & above_heatindex_pct_99 == 0, 1, 0),
    heatindex_95_ex  = if_else(above_heatindex_pct_95 == 1 & above_heatindex_pct_99 == 0, 1, 0),
    heatindex_99_ex  = if_else(above_heatindex_pct_99 == 1, 1, 0)
  ) %>% # add in heat index when you fix the tdmean issue
  dplyr::select(Date, year, cbg, CLIMDIV, 
                tmean_90_ex, tmean_95_ex, tmean_99_ex, 
                tmax_90_ex, tmax_95_ex, tmax_99_ex, 
                biv_90_ex, biv_95_ex, biv_99_ex,
                heatindex_90_ex, heatindex_95_ex, heatindex_99_ex) 
gc()

# RUCA: 
ruca <- read_excel("Data/ruca2010revised.xlsx", skip = 1) %>%
  filter(`Select State` == "NC") %>%
  mutate(tract = as.numeric(`State-County-Tract FIPS Code (lookup by address at http://www.ffiec.gov/Geocode/)`),
         ruca2010 = as.numeric(`Primary RUCA Code 2010`)) %>%
  select(tract, ruca2010)

# Create tables----

# Merge heatwaves and calculate person-days
tabdat <- hws_raw %>%
  left_join(ehf_raw, by = c("cbg", "Date")) %>%
  left_join(full_pop, by = c("cbg", "year")) %>%
  # multiply each heatwave flag by population to get person-days
  mutate(across(
    starts_with(c("tmean", "tmax", "biv", "heatindex", "EHF")),
    ~ .x * population,
    .names = "{.col}_persondays"
  ))


summary_tab <- tabdat %>%
  group_by(CLIMDIV) %>%
  summarise(
    across(
      ends_with(c("_persondays", "ex")),
      ~ sum(.x, na.rm = TRUE),
      .names = "sum_{.col}"
    )
  ) 

tidy_tab <- summary_tab %>%
  pivot_longer(
    cols = -CLIMDIV,
    names_to = "metric",
    values_to = "value"
  ) %>%
  # separate the pieces of the name
  separate(metric, into = c("stat", "metric", "intensity","in_ex", "measure"), sep = "_", extra = "merge") %>%
  tidy_tab <- tidy_tab %>%
  filter(intensity != "all") %>%
  select(-in_ex) %>%
  mutate(measure = ifelse(is.na(measure), "count", "persondays")) %>%
  pivot_wider(
    names_from = measure,
    values_from = value
  ) %>%
  select(CLIMDIV, metric, intensity, persondays, count) 

climdiv_pop <- hws_raw %>%
  left_join(full_pop, by = c("cbg", "year")) %>%
  distinct(cbg, year, population, CLIMDIV) %>%
  group_by(CLIMDIV) %>%
  summarise(
    total_pop = sum(population, na.rm = TRUE),
    .groups = "drop"
  )

tidy_tab <- tidy_tab %>%
  left_join(climdiv_pop, by = "CLIMDIV")

tidy_tab <- tidy_tab %>%
  mutate(avg_exp = persondays/total_pop)

  # Climate division aggregation
tabdat_climdiv <- tabdat %>%
  group_by(CLIMDIV) %>%
  summarize(
    across(ends_with("persondays"), sum, na.rm = TRUE),
    .groups = "drop"
  )

tabdat_region <- tabdat_climdiv %>%
  pivot_longer(
    cols = -CLIMDIV,
    names_to = "metric",
    values_to = "persondays"
  ) %>%
  pivot_wider(
    names_from = CLIMDIV,
    values_from = persondays
  ) %>%
  mutate(
    mountain = `3101` + `3102`,
    piedmont = `3103` + `3104` + `3105`,
    coastal  = `3106` + `3107` + `3108`
  ) %>%
  select(metric, mountain, piedmont, coastal)

# Climdiv average person days

# Join population totals into person-day table
tabdat_climdiv <- tabdat %>%
  group_by(CLIMDIV) %>%
  summarize(across(ends_with("persondays"), sum, na.rm = TRUE), .groups = "drop") %>%
  left_join(climdiv_pop, by = "CLIMDIV")

# Calculate average exposure days = person-days / population
tabdat_climdiv <- tabdat_climdiv %>%
  mutate(across(
    ends_with("persondays"),
    ~ .x / total_pop,
    .names = "{.col}_avgdays"
  ))

# Now aggregate to physiographic regions
tabdat_region <- tabdat_climdiv %>%
  pivot_longer(
    cols = -c(CLIMDIV, total_pop),
    names_to = "metric",
    values_to = "value"
  ) %>%
  pivot_wider(
    names_from = CLIMDIV,
    values_from = value
  ) %>%
  mutate(
    mountain = (`3101` + `3102`) / 2,  # average across mountain divisions
    piedmont = (`3103` + `3104` + `3105`) / 3,
    coastal  = (`3106` + `3107` + `3108`) / 3
  ) %>%
  select(metric, mountain, piedmont, coastal)

# Merge heatwaves with RUCA

ruca_pop <- hws_raw%>%
  left_join(full_pop, by = c("cbg", "year")) %>%
  mutate(tract = as.numeric(substr(cbg, 1, 11))) %>%
  left_join(ruca, by = "tract") %>%
  distinct(cbg, year, population, ruca2010) %>%
  group_by(ruca2010) %>%
  summarize(total_pop = sum(population, na.rm = TRUE), .groups = "drop")

tabdat_ruca <- tabdat %>%
  mutate(tract = as.numeric(substr(cbg, 1, 11))) %>%
  left_join(ruca, by = "tract") %>%
  group_by(ruca2010) %>%
  summarize(
    across(ends_with("persondays"), sum, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_longer(
    cols = -ruca2010,
    names_to = "metric",
    values_to = "persondays"
  ) %>%
  pivot_wider(
    names_from = ruca2010,
    values_from = persondays
  ) %>%
  mutate(
    urban     = `1` + `2` + `3`,
    metro     = `4` + `5` + `6`,
    smalltown = `7` + `8` + `9`,
    rural     = `10`
  ) %>%
  select(metric, urban, metro, smalltown, rural)



tabdat_ruca <- tabdat %>%
  mutate(tract = as.numeric(substr(cbg, 1, 11))) %>%
  left_join(ruca, by = "tract") %>%
  group_by(ruca2010) %>%
  summarise(
    across(
      ends_with(c("_persondays", "ex")),
      ~ sum(.x, na.rm = TRUE),
      .names = "sum_{.col}"
    )
  ) 

tidy_ruca <- tabdat_ruca %>%
  pivot_longer(
    cols = -ruca2010,
    names_to = "metric",
    values_to = "value"
  ) %>%
  # separate the pieces of the name
  separate(metric, into = c("stat", "metric", "intensity","in_ex", "measure"), sep = "_", extra = "merge") %>%
  filter(intensity != "all") %>%
  select(-in_ex) %>%
  mutate(measure = ifelse(is.na(measure), "count", "persondays")) %>%
  pivot_wider(
    names_from = measure,
    values_from = value
  ) %>%
  select(ruca2010, metric, intensity, persondays, count) 

tidy_ruca <- tidy_ruca %>%
  left_join(ruca_pop, by = "ruca2010")

tidy_ruca <- tidy_ruca %>%
  mutate(avg_exp = persondays/total_pop)

tidy_ruca_summary <- tidy_ruca %>%
  mutate(
    region = case_when(
      ruca2010 %in% c(1, 2, 3) ~ "urban",
      ruca2010 %in% c(4, 5, 6) ~ "metro",
      ruca2010 %in% c(7, 8, 9) ~ "smalltown",
      ruca2010 == 10           ~ "rural",
      TRUE ~ NA_character_
    )
  ) %>%
  group_by(metric, intensity, region) %>%
  summarise(avg_exp = mean(avg_exp, na.rm = TRUE), .groups = "drop") %>%
  unite("event_type", metric, intensity, sep = "_") %>%
  pivot_wider(
    names_from = region,
    values_from = avg_exp
  )


tidy_tab_summary <- tidy_tab %>%
  mutate(
    region = case_when(
      CLIMDIV %in% c(3101, 3102) ~ "mountain",
      CLIMDIV %in% c(3103, 3104, 3105) ~ "piedmont",
      CLIMDIV %in% c(3106, 3107, 3108) ~ "coastal",
      TRUE ~ NA_character_
    )
  ) %>%
  group_by(metric, intensity, region) %>%
  summarise(avg_exp = mean(avg_exp, na.rm = TRUE), .groups = "drop") %>%
  unite("event_type", metric, intensity, sep = "_") %>%
  pivot_wider(
    names_from = region,
    values_from = avg_exp
  )





