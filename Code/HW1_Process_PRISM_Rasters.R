
### 1_Process_PRISM_Rasters.R
### Input: PRISM daily raster temperature data (tmin, tmax, tmean, tdmean, normals)
### Source: https://prism.oregonstate.edu/recent/
### Output: 
### - PRISM_dailys_2012_2024_CBG2020.parquet
### - PRISM_dailys_2012_2024_CBG2010.parquet

# Aggregate PRISM data from raster to geographic unit and create daily time series
# for the specified time period with tmean, tmax, tmin, and tdmean (dew point)

# Adapted from https://github.com/wertisml/Heatwave/blob/main/ZIP_Code/1.PRISM.R

library(data.table)
library(rgdal)
library(sf)
library(raster)
library(tidyverse)
library(prism)
library(future)
library(furrr)
library(arrow)
library(tigris)

# Set working directory to the raw data folder - this is where the raw PRISM data 
# will go.
setwd("~/Heatwave_Mobility/Data/Temp/Raw")

#==============================================================================#
# PRISM TAVG
#==============================================================================#

dir.create("Tmean", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Tmean"))

get_prism_dailys(
  type = "tmean", 
  minDate = "2012-01-01", 
  maxDate = "2024-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM TMAX
#==============================================================================#

dir.create("Tmax", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Tmax"))

get_prism_dailys(
  type = "tmax", 
  minDate = "2012-01-01", 
  maxDate = "2024-12-31",  
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM TMIN
#==============================================================================#

dir.create("Tmin", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Tmin"))

get_prism_dailys(
  type = "tmin", 
  minDate = "2012-01-01", 
  maxDate = "2024-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM Tdmean (mean dew point temperature)
#==============================================================================#

dir.create("Tdmean", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Tdmean"))

get_prism_dailys(
  type = "tdmean", 
  minDate = "2023-03-19", 
  maxDate = "2024-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# 30-year normals
#==============================================================================#

dir.create("Normals", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Normals"))

get_prism_dailys(type = "tmean", 
                 resolution = "800m",
                 mon = NULL,
                 keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# Pre-process
#==============================================================================#

CBG2020Shape <- block_groups(state = "NC", year = "2020") %>%
  mutate(cbg=as.numeric(GEOID))%>%
  mutate(county=as.numeric(COUNTYFP))%>%
  dplyr::select(cbg, county, geometry)

CBG2010Shape <- block_groups(state = "NC", year = "2010") %>%
  mutate(cbg=as.numeric(GEOID10))%>%
  mutate(county=as.numeric(COUNTYFP))%>%
  dplyr::select(cbg, county, geometry)

# Define the list of temperature types
temp_vars <- c("Tmean", "Tmin", "Tmax", "Tdmean")

# Run this for loop using CBG2020Shape and then CBG2010Shape

# Change the working directory for this

setwd("~/Heatwave_Mobility")

# Loop over each temperature variable
for (temp_var in temp_vars) {
  
  cat("Processing:", temp_var, "\n")
  
  # Define path to raster files
  dat.files <- list.files(path = paste0(temp_var, "/"),
                          recursive = TRUE,
                          pattern = "\\_bil.bil$",
                          full.names = TRUE,
                          ignore.case = FALSE,
                          no.. = TRUE)
  
  # Create a sequence of dates starting from Jan 1, 2012
  tempo <- seq(as.Date("2012-01-01"), by = "day", length.out = length(dat.files))
  
  # Combine rasters into a stack
  mystack <- stack(dat.files)
  
  # Assign names based on dates
  r <- setNames(mystack, tempo)
  
  # Reproject shapefile to match raster CRS
  CBG2020Shape_proj <- st_transform(CBG2020Shape, st_crs(r))
  
  # Crop raster to shapefile extent
  r.crop <- crop(r, extent(CBG2020Shape_proj))
  
  # Extract mean values by polygon
  r.mean <- raster::extract(r.crop, CBG2020Shape_proj, method = "simple", fun = mean, sp = TRUE)
  
  # Convert to data.frame and add IDs
  temps <- as.data.frame(r.mean)
  temps$OBJECTID <- seq(1, nrow(temps))
  temps <- temps %>% dplyr::select(OBJECTID, everything())
  
  # Initialize empty container
  data <- data.table()
  
  # Transpose and reshape the data
  for (i in temps$OBJECTID) {
    
    first <- temps[i,]
    
    tran <- cbind(t(first[, 11:ncol(temps)]), colnames(first[, 11:ncol(temps)]))
    
    combined <- data.table(cbind(tran, first[, 2]))
    
    colnames(combined) <- c(temp_var, "Date", "cbg")
    
    comb <- combined %>%
      mutate(Date = str_sub(Date, 2, -1),
             Date = as.Date(gsub("\\.", "-", Date), format = "%Y-%m-%d"),
             !!sym(temp_var) := round(as.numeric(.data[[temp_var]]), digits = 2))
    
    data <- rbind(data, comb)
    
    if (i %% 100 == 0) cat("Processed census block group", i, "\n")  # Progress report every 100
  }
  
  # Write to parquet # CHANGE THIS WHEN YOU SWITCH TO 2010 !!!!!!!!!!!
  # Note the CBG_2020 folder location and the CBG2020 file naming -- make sure these 
  # match the geography you are working with
  write_parquet(data, paste0("Data/Temp/Processed/CBG_2020/", tolower(temp_var), "_2012_2024_CBG2020.parquet")) 
  
}


# Create merged PRISM dataset

tmin <- read_parquet("Data/Temp/Processed/CBG_2020/Tmin_2012_2024_CBG2020.parquet") 
tmax <- read_parquet("Data/Temp/Processed/CBG_2020/Tmax_2012_2024_CBG2020.parquet") 
tmean <- read_parquet("Data/Temp/Processed/CBG_2020/Tmean_2012_2024_CBG2020.parquet") 
tdmean <- read_parquet("Data/Temp/Processed/CBG_2020/Tdmean_2012_2024_CBG2020.parquet") 

data <- tmean %>%
  left_join(tmin, by=c('Date', 'cbg'))%>%
  left_join(tmax, by=c('Date', 'cbg'))%>%
  left_join(tdmean, by=c('Date', 'cbg'))

summary(is.na(data))
# Write merged file
write_parquet(data, "Data/Temp/Processed/CBG_2020/PRISM_dailys_2012_2024_CBG2020.parquet") 

tmin <- read_parquet("Data/Temp/Processed/CBG_2010/Tmin_2012_2024_CBG2010.parquet")
tmax <- read_parquet("Data/Temp/Processed/CBG_2010/Tmax_2012_2024_CBG2010.parquet")
tmean <- read_parquet("Data/Temp/Processed/CBG_2010/Tmean_2012_2024_CBG2010.parquet")
tdmean <- read_parquet("Data/Temp/Processed/CBG_2010/Tdmean_2012_2024_CBG2010.parquet")

data <- tmean %>%
  left_join(tmin, by=c('Date', 'cbg'))%>%
  left_join(tmax, by=c('Date', 'cbg'))%>%
  left_join(tdmean, by=c('Date', 'cbg'))

# Write merged file
write_parquet(data, "Data/Temp/Processed/CBG_2010/PRISM_dailys_2012_2024_CBG2010.parquet")

