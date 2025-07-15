
library(arrow)

#setwd(~Heatwave_Mobility)

# 1) SETUP -------------------------------------------------------------------
# Read in weekly visitation and EHF heatwave dataset: 
dat <- read_parquet("Data/Weekly_Series_Visits_EHF_Merge_2022_2024.parquet") %>%
  rename(cbg = poi_cbg)

summary(is.na(dat)) # Check for NA values 

# Read in census block group database: 
cbg_dbase <- read.csv("Data/CBG2010_Database.csv")%>%
  select(-X)

# Remove CBGs that never experenced a heatwave: 
dat <- dat %>%
  group_by(cbg) %>%
  mutate(total_HW = sum(EHF_HW, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(total_HW > 0) %>%
  select(-total_HW)

# Join pop variable and filter out CBGs where the population is 0
dat <- dat %>%
  left_join(cbg_dbase, by = "cbg") %>%
  filter(pop2019 > 0)

# Create time features and population-adjusted trip variable 
dat <- dat %>%
  mutate(year = year(date_range_start), # Create year variable
         month = month(date_range_start), # Create month variable
         pop_adjusted_trips = raw_visit_counts / pop2019, # Population-adjusted variable? 
         EHF_HW_binary = as.integer(EHF_HW > 0)) # Binary heatwave week indicator


# 2a) Define trip types of interest based on NAICS code ----------------------------
dat <- dat %>%
  mutate(trip_type = case_when(
    str_starts(poi_category, "Construction") |
      str_starts(poi_category, "Manufacturing") |
      str_starts(poi_category, "Agriculture, Forestry, Fishing and Hunting") |
      str_starts(poi_category, "Mining, Quarrying, and Oil and Gas Extraction") |
      str_starts(poi_category, "Utilities") |
      str_starts(poi_category, "Wholesale Trade") ~ "Anchor",
    naics_code == 712190 ~ "Park",
    naics_code == 445110 ~ "Grocery", 
    naics_code == 
    TRUE ~ NA_character_
  ))


# TEST MODELS ------------------------------------------------------------------

# Data prep for modeling -------------------------------------------------------
anchor_trips <- dat %>%
  filter(trip_type == "Anchor") %>% #Select just anchor (work, school) trips
  filter(month(date_range_start) >= 5 & month(date_range_start) <= 9) %>% # Filter to warm months
  filter(raw_visit_counts > 0) %>% # Maybe controversial but remove where weekly trips are zero
  group_by(cbg, date_range_start, pick(starts_with("EHF"))) %>%
  summarize(raw_visit_counts = sum(raw_visit_counts))

anchor_trips <- anchor_trips %>%
  left_join(cbg_dbase, by = 'cbg') %>%
  filter(pop2019 > 0)

# Calculate boundaries to filter outliers
LB <- quantile(anchor_trips$raw_visit_counts, 0.10, na.rm = TRUE)
UB <- quantile(anchor_trips$raw_visit_counts, 0.90, na.rm = TRUE)
subset_range <- UB - LB

# Define lower and upper bounds
lower_bound <- LB - 1.5 * subset_range
upper_bound <- UB + 1.5 * subset_range

# Filter out outliers
anchor_trips_no_outliers <- anchor_trips %>%
  filter(raw_visit_counts >= lower_bound & raw_visit_counts <= upper_bound)

summary(anchor_trips_no_outliers$raw_visit_counts)
hist(anchor_trips_no_outliers$raw_visit_counts)

anchor_trips_no_outliers <- anchor_trips_no_outliers %>%
  mutate(month = month(date_range_start),
         year = year(date_range_start))

table(anchor_trips_no_outliers$EHF_HW)

# Remove CBGs that experienced no heatwaves during the whole study period? 

no_hws <- anchor_trips_no_outliers %>%
  group_by(cbg) %>%
  summarize(HW = sum(EHF_HW))%>%
  filter(HW==0)
no_hws <- no_hws$cbg

anchor_trips_no_outliers <- anchor_trips_no_outliers %>%
  filter(!cbg %in% no_hws)

table(anchor_trips_no_outliers$EHF_HW)



# Generalized Linear Mixed Model -----------------------------------------------

library(lme4)
model_anchor <- glmer.nb(raw_visit_counts ~ EHF_HW_binary +
                           offset(log(pop2019)) +  # exposure/population
                           factor(month) +         # temporal control
                           factor(phys_region) +   # geography
                           factor(RUCC_2023) +     # urbanicity
                           (1 | cbg),              # random intercept by CBG
                         data = anchor_trips_no_outliers)
summary(model_anchor)
tab_model(model_anchor)

library(glmmTMB)

model_anchor <- glmmTMB(raw_visit_counts ~ EHF_HW +
                          factor(month) +
                          factor(phys_region) +
                          factor(RUCC_2023) +
                          (1 | cbg),
                        family = nbinom2,
                        data = anchor_trips_no_outliers)






