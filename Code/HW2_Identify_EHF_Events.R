
# Adapted from https://github.com/wertisml/Heatwave/blob/main/ZIP_Code/4.Heatwave_Calculation.R
# Code to identify EHF heatwave days from 2022 to 2024 at the tract level in NC

library(tidyverse)
library(arrow)
library(data.table)

# Set working directory
setwd("~/Heatwave_Mobility")

#==============================================================================#
# Read in and set up data
#==============================================================================#

# (Process 2010 CBG boundaries and then replace with 2010 CBG boundaries)

Temperature <- left_join(read_parquet("Data/Temp/Processed/CBG_2010/PRISM_dailys_2012_2024_CBG2010.parquet") %>% # Create a df of the temeprature data and rename the columns
                           mutate(month = month(Date)) %>%
                           collect(),
                         read_parquet("Data/Temp/Processed/CBG_2010/PRISM_dailys_2012_2024_CBG2010.parquet") %>% # Create a df for extreme percentile of temperatures
                           mutate(month = month(Date)) %>%
                           collect() %>%
                           group_by(cbg, month) %>%
                           do(data.frame(t(quantile(.$Tmean, probs = c(0.95, 0.97, 0.99), na.rm = TRUE))))%>%
                           rename(H95 = X95.,
                                  H97 = X97.,
                                  H99 = X99.),
                         by = c("cbg", "month")) %>%
  arrange(Date) %>%
  rename(TMIN = Tmin,
         TMAX = Tmax,
         TAVG = Tmean) %>%
  na.omit(.) # Remove any rows that contain missing values

#==============================================================================#
# Create Heatwave Functions
#==============================================================================#

# Calculate the EHI acclimation 
Three_day_average <- function(Data){
  
  # Initialize the start and end indices of the window
  start_index <- 31
  
  # Initialize a new column called is_larger
  Data[, "three_day_average"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while ((start_index+2) <= nrow(Data)) {
    
    # Get the current window of rows
    current_window <- Data[(start_index-2):(start_index), ]
    
    # Get the mean value of the current window
    current_window_mean <- mean(current_window$TAVG)
    
    # Check if the current window mean value is greater than the previous 30 rows mean value
    Data$three_day_average[start_index] <- current_window_mean 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    
  }
  return(Data)
}

# Calculate the EHI acclimation 
Three_day_95th_percentile <- function(Data){
  
  # Initialize the start and end indices of the window
  start_index <- 31
  
  # Initialize a new column called is_larger
  Data[, "three_day_95th_percentile"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while ((start_index+2) <= nrow(Data)) {
    
    # Get the current window of rows
    current_window <- Data[(start_index-2):(start_index), ]
    
    # Get the mean value of the current window
    current_window_mean <- mean(current_window$H95)
    
    # Check if the current window mean value is greater than the previous 30 rows mean value
    Data$three_day_95th_percentile[start_index] <- current_window_mean 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    
  }
  return(Data)
}

# Calculate the EHI acclimation 
Previous_30_day_mean <- function(Data){
  
  # Initialize the start and end indices of the window
  start_index <- 31
  
  # Initialize a new column called is_larger
  Data[, "Previous_30_day_mean"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while ((start_index+2) <= nrow(Data)) {
    
    # Get the previous 30 rows
    prev_30_rows <- Data[(start_index-30):(start_index-1), ]
    
    # Get the mean value of the previous 30 rows
    prev_30_rows_mean <- mean(prev_30_rows$TAVG)
    
    # Check if the current window mean value is greater than the previous 30 rows mean value
    Data$Previous_30_day_mean[start_index] <- prev_30_rows_mean 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    
  }
  return(Data)
}

Calculate <- function(Data) {
  
  Data %>%
    Three_day_average() %>%
    Three_day_95th_percentile() %>%
    Previous_30_day_mean()
}  

#==============================================================================#
# Calculate Heat Wave
#==============================================================================#

library(dplyr)
library(furrr)
library(purrr)

EHF_pipeline_chunked <- function(dataset, chunk_size = 100) {
  # Get unique cbg values and split into chunks
  cbg_chunks <- split(unique(dataset$cbg), 
                        ceiling(seq_along(unique(dataset$cbg)) / chunk_size))
  
  results <- list()
  
  # Process each chunk separately to avoid memory overload
  for (i in seq_along(cbg_chunks)) {
    message(paste("Processing chunk", i, "of", length(cbg_chunks)))
    
    subset_data <- dataset %>% filter(cbg %in% cbg_chunks[[i]])
    
    chunk_result <- subset_data %>%
      dplyr::select(cbg, Date, TAVG, H95) %>%
      nest(data = c(-cbg)) %>%
      mutate(calculate = future_map(data, Calculate, .options = furrr_options(seed = TRUE))) %>%
      dplyr::select(-data) %>%
      unnest(cols = c(calculate), names_repair = "minimal") %>%
      mutate(EHI_sig = three_day_average - three_day_95th_percentile,
             EHI_accl = three_day_average - Previous_30_day_mean,
             EHI_sig = if_else(EHI_sig < 0, 0, EHI_sig),
             EHI_accl = if_else(EHI_accl < 1, 1, EHI_accl),
             EHF = EHI_sig * EHI_accl) %>%
      dplyr::select(-three_day_95th_percentile, -three_day_average, -Previous_30_day_mean)
    
    results[[i]] <- chunk_result
    rm(subset_data, chunk_result)  # Free up memory
    gc()  # Force garbage collection
  }
  
  # Combine all processed chunks
  bind_rows(results)
}

# Run function in chunks of 100 cbgs at a time
plan(multisession, workers = (availableCores() - 1))  # Ensure parallel processing
Temperature_1 <- EHF_pipeline_chunked(Temperature, chunk_size = 100)

Temperature <- left_join(Temperature, Temperature_1 %>% dplyr::select(-TAVG, -H95), by = c("cbg", "Date")) 

#==============================================================================#
# Calculate the 85th percentile EHF
#==============================================================================#

Heatwave <- left_join(Temperature %>%
                        mutate(Severe_occurance = ifelse(EHF > 0, 1, 0),
                               daynum = 1) %>%
                        collect() %>%
                        drop_na(.),
                      Temperature %>% 
                        dplyr::select(cbg, EHF) %>%
                        group_by(cbg) %>% 
                        collect() %>%
                        filter(EHF > 0) %>%
                        do(data.frame(t(quantile(.$EHF, probs = c(0.85), na.rm=T)))) %>%
                        rename(EHF85 = X85.),
                      by = c("cbg"))

#==============================================================================#
# Calculate the daily information 
#==============================================================================#

library(humidity)

Heat_Calc <- Heatwave %>%
  group_by(cbg) %>%
  mutate(Severe_Heatwaves = (ifelse(EHF >= EHF85, 1, 0)),
         Extreme_Heatwaves = (ifelse(EHF >= (3 * EHF85), 1, 0)),
         low_intensity = (ifelse(EHF > 0 & EHF < 1, 1, 0)),
         moderate_intensity = (ifelse(EHF >= 1 & EHF < 2, 1, 0)),  
         high_intensity = (ifelse(EHF >= 2, 1, 0)),
         Above_95th = fifelse(TAVG > H95, 1, 0),
         Above_97th  = fifelse(TAVG > H97, 1, 0),
         Above_99th = fifelse(TAVG > H99, 1, 0),
         Heatwave = if_else(low_intensity > 0 | moderate_intensity > 0 | high_intensity > 0, 1, 0)) %>%
  dplyr::select(Date, cbg, TAVG, TMIN, TMAX, EHF, EHI_sig, EHI_accl, 
                Severe_Heatwaves, Extreme_Heatwaves, Above_95th, Above_97th, Above_99th, Heatwave,
                low_intensity, moderate_intensity, high_intensity)

#==============================================================================#
# Write to .parquet
#==============================================================================#

write_parquet(Heat_Calc, "Data/Temp/Processed/CBG_2010/EHF_Heatwave_NC2010CBG_2012_2024.parquet") # Repeat for 2010




