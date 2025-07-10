
library(lubridate)
library(tidyverse)

setwd("C:/Users/ulric/OneDrive - University of North Carolina at Chapel Hill/Heatwave_Mobility/")

weeklydat <- read_parquet("Data/Weekly_Patterns_POI_2022_2024.parquet")

weeklydat <- weeklydat %>%
  mutate(trip_type = case_when(
    str_starts(poi_category, "Construction") ~ "Anchor",
    str_starts(poi_category, "Manufacturing") ~ "Anchor",
    str_starts(poi_category, "Agriculture, Forestry, Fishing and Hunting") ~ "Anchor",
    str_starts(poi_category, "Mining, Quarrying, and Oil and Gas Extraction") ~ "Anchor",
    str_starts(poi_category, "Utilities") ~ "Anchor",
    str_starts(poi_category, "Wholesale Trade") ~ "Anchor",
    str_starts(poi_category, "Management of Companies and Enterprises") ~ "Anchor",
    str_starts(poi_category, "Child Care Services") ~ "Anchor",
    str_starts(poi_category, "Educational Services") ~ "Anchor",
    str_starts(poi_category, "Information") ~ "Mixed Work",
    str_starts(poi_category, "Finance and Insurance") ~ "Mixed Work",
    str_starts(poi_category, "Real Estate and Rental and Leasing") ~ "Mixed Work",
    str_starts(poi_category, "Professional, Scientific, and Technical Services") ~ "Mixed Work",
    str_starts(poi_category, "Public Administration") ~ "Mixed Work",
    str_starts(poi_category, "Scenic and Sightseeing Transportation") ~ "Discretionary",
    str_starts(poi_category, "Arts, Entertainment, and Recreation") ~ "Discretionary",
    str_starts(poi_category, "Drinking Places") ~ "Discretionary",
    str_starts(poi_category, "Restaurants and Other Eating Places") ~ "Discretionary",
    TRUE ~ "Undefined"
  ))

weeklydat <- weeklydat %>%
  mutate(trip_type = ifelse(naics_code == "712190", "Parks", trip_type))

weeklydat <- weeklydat %>%
  mutate(season = case_when(
    month(date_range_start) >= 5 & month(date_range_start) <= 9 ~ "warm",
    month(date_range_start) <= 3 | month(date_range_start) >= 11 ~ "cool",
    TRUE ~ "transition"
  ))

sub_tab <- weeklydat %>%
  group_by(sub_category, season) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = season, values_from = raw_visit_counts)

top_tab <- weeklydat %>%
  group_by(top_category, season) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = season, values_from = raw_visit_counts)

poi_tab <- weeklydat %>%
  group_by(poi_category, season) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = season, values_from = raw_visit_counts)

plot_dat1 <- weeklydat %>%
  group_by(date_range_start, season, poi_category) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE))%>%
  filter(date_range_start >= "2023-01-01")

ggplot(plot_dat1, aes(x = date_range_start, y = raw_visit_counts, color = poi_category)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(plot_dat1$date_range_start), max(plot_dat1$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

plot_dat2 <- weeklydat %>%
  group_by(date_range_start, season, trip_type) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE))%>%
  filter(date_range_start >= "2023-01-01")

parkplot <- plot_dat2 %>%
  filter(trip_type == "Parks")

ggplot(parkplot, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(parkplot$date_range_start), max(parkplot$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  ggtitle("Parks (NAICS = 712190)") + 
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

anchorplot <- plot_dat2 %>%
  filter(trip_type == "Anchor")

ggplot(anchorplot, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(anchorplot$date_range_start), max(anchorplot$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  ggtitle("anchor") + 
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

plot_dat3 <- weeklydat %>%
  filter(sub_category == "Fruit and Vegetable Markets")%>%
  group_by(date_range_start) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE))%>%
  filter(date_range_start >= "2023-01-01")

ggplot(plot_dat3, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(plot_dat1$date_range_start), max(plot_dat1$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 


