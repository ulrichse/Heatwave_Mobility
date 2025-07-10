
library(dplyr)
library(arrow)

setwd("C:/Users/ulric/OneDrive - University of North Carolina at Chapel Hill/Heatwave_Mobility/")

dat <- read_parquet("Data/Weekly_Series_Visits_HW_Merge_2022_2024.parquet")

# Missing the heat data following 2024-07-29
# Will probably have to reprocess from PRISM 

dat <- dat %>%
  filter(date_range_start <= "2024-07-29")

# Create table ? 

library(dplyr)
library(lubridate)
library(ggplot2)

# Summarize visit counts by date
plot_dat1 <- dat %>%
  group_by(date_range_start) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts, na.rm = TRUE))

# Create list of May–September intervals to highlight
highlight_months <- plot_dat1 %>%
  mutate(year = year(date_range_start)) %>%
  distinct(year) %>%
  mutate(start = as.Date(paste0(year, "-05-01")),
         end   = as.Date(paste0(year, "-09-30"))) %>%
  filter(start >= min(plot_dat1$date_range_start))

# Define max y-value for label placement
y_label_pos <- max(plot_dat1$raw_visit_counts, na.rm = TRUE) * 1.05

# Plot
ggplot() +
  # Light orange shaded background
  geom_rect(data = highlight_months,
            aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf),
            fill = "orange", alpha = 0.05) +
  
  # Add "Warm Season" label near top center of each rectangle
  annotate("text",
           x = highlight_months$start + 60,  # approx. midpoint of May–Sept
           y = y_label_pos,
           label = "Warm Season",
           angle = 0,
           size = 3.5,
           fontface = "italic",
           color = "darkorange3") +
  
  # Line plot
  geom_line(data = plot_dat1,
            aes(x = date_range_start, y = raw_visit_counts),
            size = 0.5) +
  
  # Axis formatting
  scale_x_date(breaks = seq(min(plot_dat1$date_range_start),
                            max(plot_dat1$date_range_start),
                            by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


library(dplyr)
library(lubridate)
library(ggplot2)

# Define min and max date in your data
min_date <- min(plot_dat$date_range_start)
max_date <- max(plot_dat$date_range_start)

# Create ranges for May–September for each year in data
highlight_months <- plot_dat %>%
  mutate(year = year(date_range_start)) %>%
  distinct(year) %>%
  mutate(start = as.Date(paste0(year, "-05-01")),
         end   = as.Date(paste0(year, "-09-30"))) %>%
  filter(start >= min_date & end <= max_date)

# Extract just the May 1st dates for vertical lines
may_start_lines <- highlight_months$start

# Final plot
ggplot() +
  # Light orange shaded background
  geom_rect(data = highlight_months,
            aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf),
            fill = "#FFD580", alpha = 0.03) +  # very light shading
  
  # Vertical lines on May 1st
  geom_vline(xintercept = as.numeric(may_start_lines),
             color = "orange", linetype = "dashed", linewidth = 0.4) +
  
  # Line chart
  geom_line(data = plot_dat,
            aes(x = date_range_start, y = raw_visit_counts, color = poi_category),
            size = 0.5) +
  
  scale_x_date(breaks = seq(min_date, max_date, by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))







plot_dat <- dat %>%
  group_by(date_range_start, poi_category)%>%
  summarize(across(c(raw_visit_counts), sum, na.rm = TRUE))%>%
  filter(poi_category %in% c("Child Care Services", "Educational Services", "Health Care and Social Assistance", "Food and Beverage Retailers","Arts, Entertainment, and Recreation"))

ggplot(plot_dat, aes(x = date_range_start, y = raw_visit_counts, color = poi_category)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(plot_dat$date_range_start), max(plot_dat$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 


library(dplyr)
library(lubridate)
library(ggplot2)

# Define min and max date in your data
min_date <- min(plot_dat$date_range_start)
max_date <- max(plot_dat$date_range_start)

# Create ranges for May–September for each year in data
highlight_months <- plot_dat %>%
  mutate(year = year(date_range_start)) %>%
  distinct(year) %>%
  mutate(start = as.Date(paste0(year, "-05-01")),
         end   = as.Date(paste0(year, "-09-30"))) %>%
  filter(start >= min_date & end <= max_date)

# Extract just the May 1st dates for vertical lines
may_start_lines <- highlight_months$start

# Final plot
ggplot() +
  # Light orange shaded background
  geom_rect(data = highlight_months,
            aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf),
            fill = "#FFD580", alpha = 0.03) +  # very light shading
  
  # Vertical lines on May 1st
  geom_vline(xintercept = as.numeric(may_start_lines),
             color = "orange", linetype = "dashed", linewidth = 0.4) +
  
  # Line chart
  geom_line(data = plot_dat,
            aes(x = date_range_start, y = raw_visit_counts, color = poi_category),
            size = 0.5) +
  
  scale_x_date(breaks = seq(min_date, max_date, by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))












mapdat <- counties %>%  
  left_join(county_dat, by=c('poi_county')) 

plot <- mapdat %>%
  filter(poi_category=="Food and Beverage Retailers")%>%
  filter(County %in% c("Mecklenberg", "Watauga", "Orange", "Wake", "Guilford", "New Hanover"))

ggplot(plot, aes(x = date_range_start, y = raw_visit_counts, color = County)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(plot$date_range_start), max(plot$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  ggtitle("Food and Beverage Retailers") + 
  theme_minimal() + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

mapdat2 <- mapdat %>%
  filter(poi_category=="Child Care Services")

tm_shape(mapdat2) + 
  tm_fill("raw_visit_counts", fill.scale = tm_scale_intervals(style = "jenks")) +
  tm_borders(col_alpha = 0.1)

## Explore

dat_hw <- dat_grp %>%
  filter(Heatwave > 0) %>%
  group_by(date_range_start)%>%
  summarize(across(c(raw_visit_counts, raw_visitor_counts), sum, na.rm = TRUE))

ggplot(dat_hw, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  theme_minimal() 

dat_no_hw <- dat_grp %>%
  filter(Heatwave==0) %>%
  group_by(date_range_start)%>%
  summarize(across(c(raw_visit_counts, raw_visitor_counts), sum, na.rm = TRUE))

ggplot(dat_no_hw, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  theme_minimal() 

# Combine the two datasets and add a column for heatwave status
dat_combined <- bind_rows(
  dat_hw %>% mutate(Heatwave_Status = "Heatwave"),
  dat_no_hw %>% mutate(Heatwave_Status = "No Heatwave")
)

# Plot both lines on the same graph
ggplot(dat_combined, aes(x = date_range_start, y = raw_visit_counts, color = Heatwave_Status)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(dat_combined$date_range_start), max(dat_combined$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() +
  labs(title = "Raw Visit Counts: Heatwave vs. No Heatwave",
       x = "Date",
       y = "Raw Visit Counts") +
  scale_color_manual(values = c("Heatwave" = "red", "No Heatwave" = "blue")) + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

# This kind of tells us nothing


# Grocery trips

library(dplyr)
library(arrow)
library(ggplot2)

grocery_grp <- dat %>%
  filter(poi_category=="Food and Beverage Retailers")%>%
  group_by(date_range_start, poi_tract) %>%
  summarize(across(c(Heatwave, raw_visit_counts, raw_visitor_counts), sum, na.rm = TRUE))

dat_hw <- grocery_grp %>%
  filter(Heatwave > 0) %>%
  group_by(date_range_start)%>%
  summarize(across(c(raw_visit_counts, raw_visitor_counts), sum, na.rm = TRUE))

ggplot(dat_hw, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  theme_minimal() 

dat_no_hw <- grocery_grp %>%
  filter(Heatwave==0) %>%
  group_by(date_range_start)%>%
  summarize(across(c(raw_visit_counts, raw_visitor_counts), sum, na.rm = TRUE))

ggplot(dat_no_hw, aes(x = date_range_start, y = raw_visit_counts)) +
  geom_line(size = 0.5) +
  theme_minimal() 

# Combine the two datasets and add a column for heatwave status
dat_combined <- bind_rows(
  dat_hw %>% mutate(Heatwave_Status = "Heatwave"),
  dat_no_hw %>% mutate(Heatwave_Status = "No Heatwave")
)

# Plot both lines on the same graph
ggplot(dat_combined, aes(x = date_range_start, y = raw_visit_counts, color = Heatwave_Status)) +
  geom_line(size = 0.5) +
  scale_x_date(breaks = seq(min(dat_combined$date_range_start), max(dat_combined$date_range_start), by = "1 month"),
               date_labels = "%b %Y") +
  theme_minimal() +
  labs(title = "Grocery Raw Visit Counts: Heatwave vs. No Heatwave",
       x = "Date",
       y = "Raw Visit Counts") +
  scale_color_manual(values = c("Heatwave" = "red", "No Heatwave" = "blue")) + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

# This kind of tells us nothing



