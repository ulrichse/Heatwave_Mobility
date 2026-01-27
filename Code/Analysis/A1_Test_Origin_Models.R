
library(tidyverse)
library(arrow)
library(lubridate)
library(broom)

#setwd("C:/Users/ulrichs/OneDrive - University of North Carolina at Chapel Hill/DIS/Heatwave_Mobility/")

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

# Plots of detrended visits ----

cbg_long <- dat %>%
  filter(phys_region == "Coast") %>%
  group_by(date_range_start) %>%
  summarize(
    daytime_visits_away = sum(daytime_visits_away),
    daytime_visits_away_seasonadj = sum(daytime_visits_away_seasonadj),
    .groups = "drop"
  ) %>%
  tidyr::pivot_longer(
    cols = c(daytime_visits_away, daytime_visits_away_seasonadj),
    names_to  = "series",
    values_to = "trips"
  ) %>%
  mutate(
    series = recode(
      series,
      daytime_visits_away = "Unadjusted",
      daytime_visits_away_seasonadj = "Seasonally adjusted"
    )
  )

## Plot of all weeks ----

ggplot(
  cbg_long,
  aes(
    x = date_range_start,
    y = trips,
    color = series,
    group = series
  )
) +
  geom_line(linewidth = 0.4) +
  scale_color_manual(
    values = c(
      "Seasonally adjusted" = "blue",
      "Unadjusted" = "grey40"
    )
  ) +
  scale_x_date(
    date_breaks = "3 months",
    date_labels = "%b %Y",
    limits = as.Date(c("2022-01-01", "2024-12-31"))
  ) +
  labs(
    x = "date",
    y = "device trips",
    color = NULL
  ) +
  theme_minimal()

## Plot of difference between adjusted and unadjusted values (pct) ----
 adj_plot <- dat %>%
  group_by(date_range_start) %>%
  summarize(
    daytime_visits_away = sum(daytime_visits_away),
    daytime_visits_away_seasonadj = sum(daytime_visits_away_seasonadj),
    .groups = "drop"
  )
adj_plot %>%
  mutate(
    pct_diff = 100 *
      (daytime_visits_away_seasonadj - daytime_visits_away) /
      daytime_visits_away
  ) %>%
  ggplot(aes(date_range_start, pct_diff)) +
  geom_line(color = "blue") +
  geom_hline(yintercept = 0, linetype = "dashed") +
  theme_minimal() +
  labs(y = "% difference (adjusted vs unadjusted)")

## Plot each year as a facet, warm season ---- 
cbg_long %>%
  filter(lubridate::month(date_range_start) %in% 5:9) %>%
  mutate(year = lubridate::year(date_range_start)) %>%
  ggplot(aes(date_range_start, trips, color = series)) +
  geom_line(linewidth = 0.5) +
  facet_wrap(~ year, scales = "free_x", ncol = 1) +
  scale_color_manual(
    values = c(
      "Seasonally adjusted" = "blue",
      "Unadjusted" = "grey40"
    )
  ) +
  theme_minimal()

# List of heatwave definitions ----

hw_defs <- c(
  "EHF_low", "EHF_moderate", "EHF_high",
  grep("^above", names(heat), value = TRUE)
)

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
# Table of weekly average device trips ----

results <- map_df(hw_defs, function(hw_var) {
  
  dat %>%
    mutate(
      hw_status = if_else(.data[[hw_var]] == 0, "non_hw", "hw")
    ) %>%
    group_by(hw_status) %>%
    summarise(
      n = n(),
      visits = sum(daytime_visits_away_seasonadj) / n,
      .groups = "drop"
    ) %>%
    mutate(heatwave_def = hw_var)
})

results

results_wide <- map_df(hw_defs, function(hw_var) {
  
  dat %>%
    mutate(hw = .data[[hw_var]] != 0) %>%
    summarise(
      n_hw = sum(hw),
      visits_hw = sum(daytime_visits_away_seasonadj[hw]) / n_hw,
      n_non_hw = sum(!hw),
      visits_non_hw = sum(daytime_visits_away_seasonadj[!hw]) / n_non_hw
    ) %>%
    mutate(heatwave_def = hw_var)
})

results_wide

# Create lists of cbgs without heatwaves to filter ----

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

zero_lists <- cbgs_with_zero(dat, hw_defs)

# Data filterng for model ----
# Filter data to only the weeks that contain warm months
dat <- dat %>%
  mutate(week_end = date_range_start + days(6)
  ) %>%
  filter(month(date_range_start) <= 9 & month(week_end) >= 5
  ) 

# Model function ----

run_heatwave_models <- function(
    dat,
    filter_expr = TRUE,   # allows full-sample models
    hw_defs,
    zero_lists = NULL,
    outcome = "daytime_visits_away_seasonadj",
    family = quasipoisson(link = "log")
) {
  
  dat_sub <- dat %>%
    filter({{ filter_expr }})
  
  map_df(
    hw_defs,
    function(hw_var) {
      
      # start from filtered data
      model_dat <- dat_sub
      
      # remove CBGs with zero exposure if applicable
      if (!is.null(zero_lists) && hw_var %in% names(zero_lists)) {
        model_dat <- model_dat %>%
          filter(!(cbg_num %in% zero_lists[[hw_var]]))
      }
      
      # fit model
      fit <- glm(
        as.formula(paste(outcome, "~", hw_var)),
        family = family,
        data   = model_dat
      )
      
      # tidy + extract heatwave effect
      tidy(fit) %>%
        filter(term != "(Intercept)") %>%
        mutate(
          heatwave_metric = hw_var,
          lower_ci = estimate - 1.96 * std.error,
          upper_ci = estimate + 1.96 * std.error
        )
    }
  ) %>%
    relocate(heatwave_metric)
}

# Stratify data for models ---- 

run_heatwave_models_stratified <- function(
    dat,
    strat_vars = c("phys_region", "RUCC_Cat"),
    hw_defs,
    zero_lists = NULL
) {
  
  # Full (unstratified) model
  results_full <- run_heatwave_models(
    dat,
    hw_defs = hw_defs,
    zero_lists = zero_lists
  ) %>%
    mutate(strat_var = "All", strat_level = "All")
  
  # Stratified models
  results_strat <- map_df(
    strat_vars,
    function(var) {
      
      dat %>%
        distinct(.data[[var]]) %>%
        pull() %>%
        map_df(function(level) {
          
          run_heatwave_models(
            dat,
            filter_expr = .data[[var]] == level,
            hw_defs = hw_defs,
            zero_lists = zero_lists
          ) %>%
            mutate(
              strat_var   = var,
              strat_level = level
            )
        })
    }
  )
  
  bind_rows(results_full, results_strat)
}

results_all <- run_heatwave_models_stratified(
  dat,
  hw_defs = hw_defs,
  zero_lists = zero_lists
)

write.csv(results_all, "results_all.csv")














