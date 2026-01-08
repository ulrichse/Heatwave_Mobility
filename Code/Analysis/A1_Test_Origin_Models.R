
library(tidyverse)
library(arrow)
library(lubridate)

setwd("C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility/")

# Read in datasets ----
dat <- read_parquet("Data/Mobility/Weekly_Away_Visits_2022_2024.parquet")
summary(is.na(dat))

heat <- read_parquet("Data/Temp/Clean/NC_CBG2010_Heatwaves.parquet")%>%
  filter(Date >= "2021-12-27" & Date <= "2024-12-30")%>% # Filter dates to match mobility data
  mutate(poi_cbg=as.numeric(cbg))%>% # Duplicate and rename cbg column to match mobility data for the join
  mutate(date_range_start = floor_date(Date - days(1), "week") + days(1)) 

# Group heatwaves into weekly time series ----
heat <- heat %>% 
  select(-ends_with("days")) %>%
  group_by(poi_cbg, date_range_start) %>%
  summarize(across(c(starts_with(c("EHF", "above_"))), 
                   \(x) sum(x, na.rm = TRUE), 
                   .names = "{.col}")) 
summary(is.na(heat)) # Check for NA values 

# Detrend weekly visits ----

y <- dat$daytime_visits_away
data.wts <- ts(y, start = c(2021, 52), frequency = 52)
decomp <- decompose(data.wts)
seasadj <- as.numeric(data.wts - decomp$seasonal)

dat <- dat %>%
  mutate(daytime_visits_away_seasonadj = seasadj)

# Merge datasets ----
cbg_dbase <- read.csv("Data/CBG2010_Database.csv")%>%
  select(-X)

dat <-dat %>%
  left_join(heat, by=c("cbg_num"="poi_cbg", "date_range_start")) %>%
  left_join(cbg_dbase, by=c("cbg_num"="cbg")) %>%
  mutate(across(where(is.numeric), ~ replace_na(.x, 0))) %>%
  filter(pop2019 > 0) %>% # Filter out CBGs where population is 0
  mutate(year = year(date_range_start), # Create year variable
         month = month(date_range_start), # Create month variable
         pop_adjusted_trips = daytime_visits_away_seasonadj / pop2019) # Population-adjusted variable? 

# Remove CBGs that are water: 
# dat <- dat %>%
  # filter(ALAND10 > 0) # Should still do this but forgot where this came from

# Create lists of cbgs to filter ----

filter_vars <- c("EHF_any", "above_tmin_pct_90", "above_tmean_pct_90", "above_tmax_pct_90", "above_tmin_tmax_90")

cbgs_with_zero <- function(dat, vars, group_var = "cbg_num") {
  
  map(vars, function(v) {
    dat %>%
      group_by(.data[[group_var]]) %>%
      summarise(
        total = sum(.data[[v]], na.rm = TRUE),
        .groups = "drop"
      ) %>%
      filter(total == 0) %>%
      pull(.data[[group_var]])
  }) %>%
    set_names(vars)
}

zero_lists <- cbgs_with_zero(dat, filter_vars)


# TEST MODELS ------------------------------------------------------------------

heatwave_definitions <- dat %>% # Create a list of heatwave defintions to model
  select(EHF_low, EHF_moderate, EHF_high, starts_with("above")) %>%
  colnames()

run_heatwave_models <- function(dat, heatwave_vars, zero_lists = NULL,
                                warm_months = 5:9, # Filter to warm season (May to September)
                                outcome = "daytime_visits_away_seasonadj", # Use seasonally adjusted visits
                                family = poisson(link = "log")) {
 
  map(heatwave_vars, function(hw_var) {
    
    model_dat <- dat %>%
      filter(month(date_range_start) %in% warm_months) %>% # Filter for warm months
      filter(.data[[outcome]] > 0) # Filter for non-zero weekly trips
    
    # Remove CBGs without any heatwave events during the study period
    if(!is.null(zero_lists) && hw_var %in% names(zero_lists)) {
      model_dat <- model_dat %>%
        filter(!(cbg_num %in% zero_lists[[hw_var]]))
    }
    
    # Aggregate weekly visits and heatwave days
    model_dat <- model_dat %>%
      group_by(cbg_num, date_range_start, .data[[hw_var]]) %>%
      summarize(
        visits = sum(.data[[outcome]], na.rm = TRUE),
        hw_days = sum(.data[[hw_var]], na.rm = TRUE),
        .groups = "drop"
      )
    
    # The actual model
    fit <- glm(visits ~ hw_days,
               family = family,
               data = model_dat)
    
    return(fit)
    
  }) %>%
    set_names(heatwave_vars)
}

heatwave_models <- run_heatwave_models(dat, heatwave_definitions, zero_lists)

library(broom)

model_results_table <- map_df(heatwave_models, function(fit) {
  tidy(fit) %>% 
    filter(term == "hw_days") %>%  # only keep the heatwave effect
    mutate(
      lower_ci = estimate - 1.96 * std.error,
      upper_ci = estimate + 1.96 * std.error
    )
}, .id = "heatwave_metric")









