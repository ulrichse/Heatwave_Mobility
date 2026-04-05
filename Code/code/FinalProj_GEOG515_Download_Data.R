 
### SETUP ----
### LOAD LIBRARIES
library(data.table)
library(sf)
library(tidyverse)
library(prism)
library(arrow)
library(tigris)
library(weathermetrics)
library(exactextractr)
library(terra)

### LOAD PRISM DATA ----
### My approach to doing this is to avoid having a bunch of PRISM data stored on 
### my computer because it was a struggle. For each of the three PRISM variables
### of interest, the function reads in all the days from a single year, processes
### those, and then deletes the PRISM files. The output is a dataset partitioned
### by year that I can then merge into a single .parquet file for heatwave processing.

### PROCESS PRISM RASTERS ----
### Read in the PRISM rasters, and average the raster values for TMIN, TMAX, and 
### TDMEAN inside of census block groups (2020 boundaries).

### Load CBGs: ----
CBG2020Shape <- block_groups(state = "NC", year = "2020") %>%
  mutate(CBG=as.numeric(GEOID))%>%
  dplyr::select(CBG, geometry)

### Define the list of temperature types:
temp_vars <- c("tmin", "tmax", "tdmean")

### Specify years: 
years <- 1981:2025

### For each temperature variable, crop the raster to the extent of NC 2020 CBG 
### boundaries, and calculate the average value for each metric within each CBG.

for (temp_var in temp_vars) {
  
  cat("Processing:", temp_var, "\n")
  
  ### Create output directory: 
  out_dir <- paste0("data/raw/", temp_var, "_dataset/")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  for (yr in years) {
    
    cat("Processing Year:", yr, "\n")
    
    start_date <- paste0(yr, "-01-01")
    end_date   <- paste0(yr, "-12-31")
    
    ### Get PRISM data for that year: 
    get_prism_dailys(
      type = temp_var,
      minDate = start_date,
      maxDate = end_date,
      keepZip = FALSE
    )
    
    ### List of files that were just downloaded: 
    dat.files <- list.files(
      path = out_dir,
      recursive = TRUE,
      pattern = "\\.bil$",
      full.names = TRUE
    )
    
    ### Extract dates: 
    file_dates <- str_extract(basename(dat.files), "\\d{8}")
    file_dates <- as.Date(file_dates, format = "%Y%m%d")
    
    ### Stack rasters: 
    r <- rast(dat.files)
    names(r) <- as.character(file_dates)
    
    ### Reproject: 
    CBG_proj <- st_transform(CBG2020Shape, st_crs(r))
    
    ### Crop to projected CBG:
    r_crop <- crop(r, ext(CBG_proj))
    
    ### Use exact_extract to extract the means, much faster than terra()
    ### Documentation here: https://tmieno2.github.io/R-as-GIS-for-Economists/extracting-values-from-raster-layers-for-vector-data.html
    r_mean <- exact_extract(r_crop, CBG_proj, "mean")
    
    ### Convert to long format ----
    temps <- as.data.frame(r_mean)
    temps$CBG <- CBG_proj$CBG
    temps <- temps %>% dplyr::select(CBG, everything())
    
    names(temps) <- sub("^mean\\.", "", names(temps))
    
    temps_dt <- as.data.table(temps)
    
    long_data <- melt(
      temps_dt,
      id.vars = "CBG",
      variable.name = "Date",
      value.name = temp_var
    )
    
    long_data[, year := yr]
    
    write_dataset(
      long_data,
      path = out_dir,
      format = "parquet",
      partitioning = "year"
    )
    
    ### DELETE ALL FILES FOR THIS YEAR 
    unlink(list.files(out_dir, full.names = TRUE), recursive = TRUE)
    
    cat("Finished year:", yr, "\n")
  }
  
  ds <- open_dataset(out_dir)
  
  merged <- ds %>% collect()
  
  write_parquet(merged, paste0("data/processed/", temp_var, "_1981_2025_CBG2020.parquet"))
  
}

### READ IN THE PROCESSED PARQUET FILES FOR TMIN, TMAX, AND TDMEAN:----
### Make sure Date is not a factor, convert to as.Date
tmin <- open_dataset("data/processed/tmin_1981_2025_CBG2020.parquet") %>%
  mutate(Date = as.Date(Date))
tmax <- open_dataset("data/processed/tmax_1981_2025_CBG2020.parquet")%>%
  mutate(Date = as.Date(Date))
tdmean <- open_dataset("data/processed/tdmean_1981_2025_CBG2020.parquet")%>%
  mutate(Date = as.Date(Date))

### COMBINE INTO SINGLE TEMP DATAFRAME: ----
data <- tdmean %>%
  left_join(tmin, by=c('Date', 'CBG'))%>%
  left_join(tmax, by=c('Date', 'CBG')) %>%
  collect()

### Inspect
summary(is.na(data))

### ADD TMEAN VARIABLE: ----
data <- data %>%
  mutate(tmean = rowMeans(cbind(tmin, tmax), na.rm = TRUE))

### CALCULATE HEAT INDEX: ----

data$heatindex <- heat.index(t = data$tmax, 
                             dp = data$tdmean, 
                             temperature.metric = "celsius", 
                             output.metric = "celsius")

data <- data %>%
  select(-year.x, -year.y)

### WRITE THE FULLY MERGED FILE: ----
write_parquet(data, "data/processed/PRISM_Dailys_1981_2025_CBG2020.parquet")





