
### SETUP ----
### Load libraries: 
library(arrow)
library(tidyverse)
library(broom)
library(tmap)

### Read in merged heatwave mobility data:
dat <- read_parquet("data/clean/Heatwave_Mobility_GEOG515.parquet")

### CREATE TABLES ----

### List of heatwave defintions: 
hw_defs <- c(
  "EHF_low", "EHF_moderate", "EHF_high",
  grep("^above", names(dat), value = TRUE)
)

### Table indicating total and average trips or visits during heatwave and non-heatwave
### weeks for each heatwave defintion: 
results_wide <- map_df(hw_defs, function(hw_var) {
  
  dat %>%
    mutate(hw = .data[[hw_var]] != 0) %>%
    summarise(
      n_hw = sum(hw),
      n_non_hw = sum(!hw),
      
      VISITS_MA365_hw = sum(VISITS_MA365[hw], na.rm = TRUE),
      VISITS_MA365_non_hw = sum(VISITS_MA365[!hw], na.rm = TRUE),
      
      AVG_VISITS_MA365_hw = mean(VISITS_MA365[hw], na.rm = TRUE),
      AVG_VISITS_MA365_non_hw = mean(VISITS_MA365[!hw], na.rm = TRUE),
      
      TRIPS_MA52w_hw = sum(TRIPS_MA52w[hw], na.rm = TRUE),
      TRIPS_MA52w_non_hw = sum(TRIPS_MA52w[!hw], na.rm = TRUE),
      
      AVG_TRIPS_MA52w_hw = mean(TRIPS_MA52w[hw], na.rm = TRUE),
      AVG_TRIPS_MA52w_non_hw = mean(TRIPS_MA52w[!hw], na.rm = TRUE)
    ) %>%
    mutate(heatwave_def = hw_var)
})

write.csv(results_wide, "data/outputs/Table1_GEOG515_FinalProj.csv")

### PLOTS OF ADJUSTED VALUES: ----
trips_weekly <- dat %>%
  group_by(DATE_RANGE_START) %>%
  summarize(
    TRIP_COUNT = sum(TRIP_COUNT, na.rm = TRUE),
    TRIPS_MA52w = sum(TRIPS_MA52w, na.rm = TRUE),
    TRIPS_DIFF = sum(TRIP_DIFF, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(DATE_RANGE_START >= as.Date("2022-01-01"))

ggplot(trips_weekly, aes(x = DATE_RANGE_START)) +
  geom_line(aes(y = TRIP_COUNT, color = "Unadjusted Device Trips from Origin CBG"), size = 1) +
  geom_line(aes(y = TRIPS_MA52w, color = "Adjusted Trips (365-day MA)"), size = 1) +
  labs(
    title = "Aggregated Warm Season Weekly Device Trips (2022-2025)",
    subtitle = "Origin CBG-based", 
    x = "Week Start Date",
    y = "Visit Count"
  ) +
  theme_minimal()+
  theme(
    plot.subtitle = element_text(face = "italic")  # ensures italics
  )

ggplot(trips_weekly, aes(x = DATE_RANGE_START)) +
  geom_line(aes(y = TRIPS_DIFF, color = "Residual"), size = 1) +
  labs(
    title = "Residual Difference between Raw and Adjusted Warm Season Weekly Device Trips (2022-2025)",
    subtitle = "Origin CBG-based; Seasonal adjustment with 52 week moving average", 
    x = "Week Start Date",
    y = "Difference"
  ) +
  theme_minimal()+
  theme(
    plot.subtitle = element_text(face = "italic")  # ensures italics
  )

### MODELS ----
### Create lists of CBGs that do not experience each type of heatwave 

cbgs_with_zero <- function(dat, vars, group_var = "CBG") {
  
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

### Model function: 

results <- map_df(hw_defs, function(hw_var) {
  
  model_dat <- dat
  
  if (!is.null(zero_lists) && hw_var %in% names(zero_lists)) {
    model_dat <- model_dat %>%
      filter(!(CBG %in% zero_lists[[hw_var]]))
  }
  
  if (nrow(model_dat) == 0 || length(unique(model_dat[[hw_var]])) < 2) {
    return(NULL)
  }
  
  fit <- glm(
    as.formula(paste("TRIPS_MA52w", "~", hw_var)),
    family = quasipoisson(link = "log"),
    data = model_dat
  )
  
  tidy(fit) %>%
    filter(term != "(Intercept)") %>%
    mutate(
      heatwave_metric = hw_var,
      lower_ci = estimate - 1.96 * std.error,
      upper_ci = estimate + 1.96 * std.error,
      IRR = exp(estimate),
      IRR_low = exp(lower_ci),
      IRR_high = exp(upper_ci)
    )
})

write.csv(results, "data/outputs/MODEL_VISITS_MA52W_GEOG515_FINALPROJ.csv") # Repeat for TRIPS_MA52W

### MAPS ----

### HEATWAVE SMALL MULTIPLES ----

#### SUMMARIZE ACROSS STUDY PERIOD ----
heat_long <- dat %>%
  group_by(CBG) %>%
  summarize(across(all_of(hw_defs), sum, na.rm = TRUE)) %>%
  pivot_longer(
    cols = all_of(hw_defs),
    names_to = "hw_def",
    values_to = "hw_days"
  )

#### LOAD 2020 CBGS ----
CBG2020Shape <- block_groups(state = "NC", year = "2020") %>%
  mutate(CBG=as.numeric(GEOID))%>%
  dplyr::select(CBG, geometry)
 
#### RECODE HW NAMES ----

### I want to make the names appear nicer, so I am using recode(). These names
### will be the panel labels that will appear in my small multiples figure.

heat_long <- heat_long %>%
  mutate(
    hw_def = recode(
      hw_def,
      above_tmean_pct_90 = "TMEAN > 90th",
      above_tmean_pct_95 = "TMEAN > 95th",
      above_tmean_pct_99 = "TMEAN > 99th",
      above_tmax_pct_90  = "TMAX > 90th",
      above_tmax_pct_95  = "TMAX> 95th",
      above_tmax_pct_99  = "TMAX > 99th",
      above_tmin_tmax_90 = "BIVARIATE > 90th",
      above_tmin_tmax_95 = "BIVARIATE > 95th",
      above_tmin_tmax_99 = "BIVARIATE > 99th",
      above_heatindex_pct_90  = "HEAT INDEX > 90th",
      above_heatindex_pct_95  = "HEAT INDEX > 95th",
      above_heatindex_pct_99  = "HEAT INDEX > 99th",
      EHF_low = "EHF Low",
      EHF_moderate = "EHF Moderate",
      EHF_high = "EHF High"
    )
  )

#### FACTOR AND LEVEL HW DEF VARIABLE ----

### Here I have to set the order of my variables so that I can map the arrangement
### of intensities in my desired visual format (left to right is low to high). 
### My choice of how to do this is to turn hw_def into a factor variable with levels. 

heat_long <- heat_long %>%
  mutate(
    hw_def = factor(
      hw_def,
      levels = c(
        # TMEAN
        "TMEAN > 90th",
        "TMEAN > 95th",
        "TMEAN > 99th",
        
        # TMAX
        "TMAX > 90th",
        "TMAX> 95th",
        "TMAX > 99th",
        
        # BIVARIATE
        "BIVARIATE > 90th",
        "BIVARIATE > 95th",
        "BIVARIATE > 99th",
        
        # HEAT INDEX
        "HEAT INDEX > 90th",
        "HEAT INDEX > 95th",
        "HEAT INDEX > 99th",
        
        # EHF (already ordinal)
        "EHF Low",
        "EHF Moderate",
        "EHF High"
      )
    )
  )

### MAKE SPATIAL ----
heat_long_sf <- CBG2020Shape %>%
  left_join(heat_long, by = "CBG")

#### CREATE MAP OF HEATWAVE DAYS ----

### Mapping using tm_facets allows me to plot small multiples!
### I have been doing previous small multiples in ggplot so I was excited to 
### figure out how to do it in tmap. 

heat_map <- tm_shape(heat_long_sf) +
  tm_polygons(
    fill = "hw_days",                     # Variable to fill polygons
    fill.scale = 
      tm_scale_continuous(                # Specify continuous color scale
        values = "matplotlib.yl_or_rd"),  # Use yellow-orange-red palette
    fill.free = FALSE,                    # Allows for one legend for all maps
    lwd = 0,                              # CBGs are so small, remove boundary visibility
    fill.legend = tm_legend(
      title = "Count",                    # Legend title
      title.size = 0.7,                   # Legent title size
      frame = FALSE                       # Remove frame around legend
    ))+
  
  ### With tm_facets, I am saying that I want to group hw_days by hw_def so that
  ### there is one map for each value of hw_def. I know that each definition has
  ### three intensity levels, and I want to display the multiple such that 
  ### each row is a metric (TMAX, TMEAN, etc.) and each column is an intensity 
  ### level (low/90th to medium/95th to high/99th)
  
  tm_facets(
    by = "hw_def",                         # Variable to distinguish maps by
    ncol = 3
  ) + 
  
  tm_layout(
    frame = FALSE,                        # I do not like frame around map
    panel.show = TRUE,                    # Indicate each map's heatwave definition 
    panel.label.bg = FALSE,               # Remove background color to panel labels
    panel.label.frame = FALSE,            # Remove frame around panel labels
    panel.label.size = 0.6                # Size of panel label text
  ) +
  
  tm_title(
    text = "Heatwave Days in North Carolina (2022-2025, Warm Season)"
  ) 

#### SAVE HEATWAVE MAP ----

tmap_save(heat_map,
          filename = "figures/Heatwaves_Small_Multiple.png",
          dpi = 144)












### AVERAGE DEVICE VISITS (TRIPS) ----

mobility_sf <- dat %>%
  group_by(CBG) %>%
  summarize(VISITS_MA365 = sum(VISITS_MA365, na.rm = TRUE),
            AVG_VISITS_MA365_hw = mean(VISITS_MA365, na.rm = TRUE),
            TRIPS_MA52w = sum(TRIPS_MA52w, na.rm = TRUE),
            AVG_TRIPS_MA52w = mean(TRIPS_MA52w, na.rm = TRUE))

mobility_sf <- mobility_sf %>%
  left_join(CBG2020Shape, by = "CBG")

#### LOAD COUNTY POLYGONS ----

### Load county polygons so that I can overlay county boundaries onto my output
### map, I think this would look nice and help the viewer orient themselves. 
counties <- counties(state = "NC") %>%
  select(geometry)

#### REPROJECT CRS ----

### Make sure everything is in the correct CRS for North Carolina. 
counties %<>% st_transform(32119)
mobility_sf %<>% st_transform(32119)

#### MAPPING SETUP ----

### AVG_VISITS_MA365 ----

### Set map breaks, corresponding labels, corresponding colors: 
map_breaks <- c(-Inf, 200, 1000, 10000, 25000, Inf)
map_labels <- c("1-200", "201-1,000", "1,001-10,000", "10,001-25,000", "25,000+")

### Set the map title: 
map_title <- "Average Weekly Device Visits to Destination CBG \nNorth Carolina (Warm Season, 2022-2024)"

#### CREATE MAP ----
mobility_map <- tm_shape(mobility_sf) +                  
  tm_polygons(fill = "AVG_VISITS_MA365",             # Specify fill variable
              fill.scale = 
                tm_scale_intervals(                   # Specify interval scale 
                  breaks = map_breaks,                # Use manually defined breaks
                  labels = map_labels,                # Use manually defined labels
                  values = "brewer.blues"),           # Use brewer blues (I am bad at colors)
              fill_alpha = 0.8,                       # Make slightly transparent for fun
              lwd = 0,                                # CBGs are small so remove boundary visibility
              fill.legend = 
                tm_legend(
                  title = "Average Weekly Visit Count",     # Legend title
                  position = c("bottom", "left"),     # Legend position
                  title.size = 0.6,                   # Legend title size  
                  text.size = 0.6,                    # Legend label size
                  frame = FALSE,               # Blend in with map background
                  
                )
  ) + 
  tm_shape(counties) +                                # Add county polygons  
  tm_polygons(fill = "white",                         # Fill them with solid white, but...
              fill_alpha = 0,                         # Make this completley transparent so you can't see it
              col = "white",                          # Make the borders white
              lwd = 0.7) +                            # Make the borders relatively thick
  tm_title_in(
    text = map_title,                                 # Add map title
    size = 0.8,                                       # Map title size
    position = "top"                                  # Map title position
  ) + 
  tm_layout(frame = FALSE) +                   # Set map background color; I want something other than the default white
  
  tm_credits(text = "Sarah Ulrich")                   # Add credits

#### SAVE MOBILITY MAP ----
tmap_save(mobility_map,
          filename = "Figures/GEOG515_avg_visits_ma365.png",
          #     width = 1500,
          dpi = 144)

### AVG_TRIP_COUNT_MA52w ----

### Set map breaks, corresponding labels, corresponding colors: 
map_breaks <- c(-Inf, 200, 1000, 10000, 25000, Inf)
map_labels <- c("1-200", "201-1,000", "1,001-10,000", "10,001-25,000", "25,000+")

### Set the map title: 
map_title <- "Average Weekly Device Trips by Origin CBG \nNorth Carolina (Warm Season, 2022-2024)"

#### CREATE MAP ----
mobility_map <- tm_shape(mobility_sf) +                  
  tm_polygons(fill = "AVG_TRIP_COUNT_MA52w",             # Specify fill variable
              fill.scale = 
                tm_scale_intervals(                   # Specify interval scale 
                  breaks = map_breaks,                # Use manually defined breaks
                  labels = map_labels,                # Use manually defined labels
                  values = "brewer.blues"),           # Use brewer blues (I am bad at colors)
              fill_alpha = 0.8,                       # Make slightly transparent for fun
              lwd = 0,                                # CBGs are small so remove boundary visibility
              fill.legend = 
                tm_legend(
                  title = "Average Weekly Trip Count",     # Legend title
                  position = c("bottom", "left"),     # Legend position
                  title.size = 0.6,                   # Legend title size  
                  text.size = 0.6,                    # Legend label size
                  frame = FALSE,               # Blend in with map background
                  
                )
  ) + 
  tm_shape(counties) +                                # Add county polygons  
  tm_polygons(fill = "white",                         # Fill them with solid white, but...
              fill_alpha = 0,                         # Make this completley transparent so you can't see it
              col = "white",                          # Make the borders white
              lwd = 0.7) +                            # Make the borders relatively thick
  tm_title_in(
    text = map_title,                                 # Add map title
    size = 0.8,                                       # Map title size
    position = "top"                                  # Map title position
  ) + 
  tm_layout(frame = FALSE) +                   # Set map background color; I want something other than the default white
  
  tm_credits(text = "Sarah Ulrich")                   # Add credits

#### SAVE MOBILITY MAP ----
tmap_save(mobility_map,
          filename = "Figures/PGEOG_515_avg_trip_count_ma52w.png",
          #     width = 1500,
          dpi = 144)
