
# Adapted from https://github.com/wertisml/Heatwave/blob/main/ZIP_Code/1.PRISM.R

library(data.table)
library(dplyr)
library(rgdal)
library(sf)
library(raster)
library(purrr)
library(tidyverse)
library(prism)
library(raster)
library(future)
library(furrr)
library(tidyverse)
library(arrow)

# Set working directory
setwd("~/Heatwave_Mobility")

# Create a 'Data' directory inside 'Heatwave_Mobility'
dir.create("Data", showWarnings = FALSE)

#==============================================================================#
# PRISM TAVG
#==============================================================================#

dir.create("Tmean", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Tmean"))

get_prism_dailys(
  type = "tmean", 
  minDate = "2012-01-01", 
  maxDate = "2024-07-31", 
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
  maxDate = "2024-07-31",  
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
  maxDate = "2024-07-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM DewPoint
#==============================================================================#

dir.create("Tdmean", showWarnings = FALSE)
prism_set_dl_dir(file.path(getwd(), "Tdmean"))

get_prism_dailys(
  type = "tdmean", 
  minDate = "2012-01-01", 
  maxDate = "2024-07-31", 
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
#Pre-process
#==============================================================================#

library(tigris)

TractShape <- tracts(state = "NC", year = "2020")%>%
  mutate(tract=as.numeric(GEOID))%>%
  mutate(county=as.numeric(COUNTYFP))%>%
  dplyr::select(tract, county, geometry)

dat.files  <- list.files(path="Tmean/", #CHANGE THIS
                         recursive=T,
                         pattern="\\_bil.bil$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)

#Change the date to be the start of the raster, length.out is the number of rasters in the list
tempo <- seq(as.Date("2012/01/01"), by = "day", length.out = length(dat.files)) 

#==============================================================================#
#Brick the rasters and cut up the rasters
#==============================================================================#

#combine all rasters into one
mystack <- stack(dat.files)

#Change the file names
r <- setNames(mystack, tempo)

#make the shapefile have the same spatial extent as the raster
TractShape <- st_transform(TractShape, st_crs(r))

#alter raster to shapefile shape
r.crop <- crop(r, extent(TractShape))

#get the values for everyhting in the brick
r.mean = raster::extract(r.crop, TractShape, method = "simple", fun = mean, sp = T)

# Write results
temps <- as.data.frame(r.mean)

temps$OBJECTID <- seq(1, nrow(temps))
temps <- temps %>%
  dplyr::select(OBJECTID, everything())

#==============================================================================#
#For loop to transpose the data and create a new file
#==============================================================================#

data = NULL
data <- data.table(data)
n = 1
for(i in temps$OBJECTID){
  if(i == n){
    
    first <- temps[i,]
    
    tran <- cbind(t(first[,11:ncol(temps)]), colnames(first[,11:ncol(temps)]))
    
    combined <- data.table(cbind(tran, first[,2]))
    
    colnames(combined) <- c("tmean", "Date", "Tract") #CHANGE THIS
    
    comb <- combined %>%
      mutate(Date = str_sub(Date, 2, -1),  # Remove leading "X"
             Date = as.Date(gsub("\\.", "-", Date), format = "%Y-%m-%d"),  # Convert to Date
             tmean = round(as.numeric(tmean), digits = 2))  # CHANGE THIS
    
    n = n+1
    print(n)
    data <- rbind(data, comb)
  }
}

library(arrow)
write_parquet(data, "Data/tmean_2012_2024.parquet") # Iterate through for tmin, tmax, tmean following #CHANGE THIS tag

# Create merged PRISM dataset

tmin <- read_parquet("Data/tmin_2012_2024.parquet")
tmax <- read_parquet("Data/tmax_2012_2024.parquet")
tmean <- read_parquet("Data/tmean_2012_2024.parquet")

data <- tmean %>%
  left_join(tmin, by=c('Date', 'Tract'))%>%
  left_join(tmax, by=c('Date', 'Tract'))

# Write merged file
write_parquet(data, "Data/NC_Tract_PRISM_2012_2024_Daily.parquet")

