

library(tigris)
library(arrow)
library(sf)
library(dplyr)

setwd("~/Heatwave_Mobility")

Heat_Calc <- read_parquet("Data/EHF_Heatwave_Metrics_NC_Tract_2012_2024.parquet")

Heat_Calc[is.na(Heat_Calc)] <- 0

Temp_grp <- Heat_Calc %>%
  group_by(Tract) %>%
  summarize(across(c(Heatwave, low_intensity, moderate_intensity, high_intensity), sum, .names = "{.col}"))%>%
  mutate(Tract=as.numeric(Tract))

tracts <- tracts(state="NC", year="2020")%>%
  mutate(Tract=as.numeric(GEOID))%>%
  dplyr::select(Tract, geometry)

tracts <- tracts %>%
  left_join(Temp_grp, by=c('Tract'))

library(tmap)
library(sf)

tm_shape(tracts) +
  tm_fill("Heatwave")

tm_shape(tracts) +
  tm_fill("low_intensity")

tm_shape(tracts) +
  tm_fill("moderate_intensity")

tm_shape(tracts) +
  tm_fill("high_intensity")

write_sf(tracts, "NC_Tract_EHF_Heatwaves_Aggregated_2012_2024.shp")
