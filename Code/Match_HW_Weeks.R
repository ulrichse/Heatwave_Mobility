
library(dplyr)
library(arrow)
library(lubridate)
library(data.table)
library(haven)
library(sjPlot)
library(MASS)
library(grid)
library(dlnm)
library(lme4)

setwd("C:/Users/ulric/OneDrive - University of North Carolina at Chapel Hill/Heatwave_Mobility/")

# Set up data for matching

data <- read_parquet("Data/Weekly_Series_Visits_HW_Merge_2022_2024.parquet")%>%
  mutate(date=as.Date(date_range_start),
         tract=poi_tract)%>%
  group_by(tract, date, poi_category)%>%
  #filter(month(date) >= 5 & month(date) <= 9)%>%
  summarize(across(c(raw_visit_counts, raw_visitor_counts), sum, na.rm = TRUE))%>%
  mutate(year=year(date),
         doy=yday(date),
         dow=wday(date))%>%
  setorder(tract, date, poi_category)

heat <- read_parquet("Data/Weekly_Series_Visits_HW_Merge_2022_2024.parquet")%>%
  mutate(date=as.Date(date_range_start))%>%
  dplyr::select(tract, date_range_start, date, Heatwave, low_intensity, moderate_intensity, high_intensity, Severe_Heatwaves, Extreme_Heatwaves)

dat <- heat %>%
  left_join(data, by=c('tract', 'date'))%>%
  setorder(tract, date)
data[is.na(data)]<-0

no_hw <- dat %>%
  group_by(tract)%>%
  summarize(hw=sum(Heatwave))%>% # Create list of tracts without heatwaves
  filter(hw==0)%>%
  dplyr::select(tract)

dat2 <- dat %>% # Remove tracts without heatwaves from the data
  filter(!tract %in% no_hw$tract)%>%
  setorder(tract, date)

gc()

##### Match heatwave-exposed WEEKS to compariable unexposed WEEKS ######

#### For lag-2 to lag7 ####

set.seed(123)
zip_list <- unique(dat$zip)

for(i in 1:length(zip_list)){
  df <- subset(dat, zip == zip_list[i])
  
  # exclude the 3 days within any other heatwave
  df$time <- 1:nrow(df)
  cand_control <- unique(c(which(df$heatwave == 1) , which(df$heatwave == 1) + 1,
                           which(df$heatwave == 1) - 1))
  df$cand_control <- TRUE
  df$cand_control[cand_control] <- FALSE
  
  # exclude the two weeks following 2001-9-11
  two_week_911 <- seq(as.Date("2001-09-11"), as.Date("2001-09-11") + 14, by = 1)
  df$excu_911 <- ifelse(df$date %in% two_week_911, FALSE, TRUE)
  
  case_dates <- subset(df, heatwave == 1)
  control_dates <- subset(df, cand_control == TRUE & excu_911 == TRUE) # Modified
  
  for(j in 1:nrow(case_dates)){
    # choose lags (lagged -2 to lagged 7)
    lag_dates <- case_dates[j, ]$date + -2:7
    lag_case <- subset(df, date %in% lag_dates)
    
    # choose 10 comparable unexposed days for each heatwave-exposed day
    control_range <- case_dates[j, ]$doy + -3:3
    control_subset <- subset(control_dates,
                             control_dates$year != case_dates[j, ]$year &
                               doy %in% control_range)
    
    # Check if there are enough controls
    if(nrow(control_subset) < 3){
      next # Skip this iteration if there are not enough controls
    }
    
    controls <- dplyr::sample_n(control_subset, 3)
    
    # choose lagged days for selected unexposed days
    la_con <- c(-2:-1, 1:7)
    lag_control <- NULL
    for(p in 1:length(la_con)){
      lag_control_dates <- controls$date + la_con[p]
      lag_control_each <- subset(df, date %in% lag_control_dates & cand_control == TRUE) # Modified
      
      if(nrow(lag_control_each) == 0){
        next # Skip this lag if no valid control days
      }
      
      if(is.null(lag_control)){
        lag_control <- lag_control_each
      }else{
        lag_control <- rbind(lag_control, lag_control_each)
      }
    }
    
    # Combine lag_case, controls, and lagged controls
    j_stratum <- rbind(lag_case, controls, lag_control)
    
    # Dynamically set the 'status' and 'lag' vectors based on actual data size
    num_lag_case <- nrow(lag_case)
    num_controls <- nrow(controls)
    num_lag_control <- nrow(lag_control)
    
    stratum <- paste("stratum", j, sep = ".")
    j_stratum$stratum <- stratum
    
    status <- c(rep("case", num_lag_case), rep("control", num_controls + num_lag_control))
    j_stratum$status <- status
    
    lag <- c(-2:7, rep(0, num_controls), rep(c(-2:-1, 1:7), each = num_controls))
    j_stratum$lag <- lag[1:nrow(j_stratum)]  # Ensure lag vector matches row count
    
    if(j == 1){
      new_df <- j_stratum
    }else{
      new_df <- rbind(new_df, j_stratum)
    }
  }
  
  if(i == 1){
    matched_df <- new_df
  }else{
    matched_df <- rbind(matched_df, new_df)
  }
}

#### For lag0 to lag7 ####

set.seed(123)
zip_list <- unique(dat$zip)

for(i in 1:length(zip_list)){
  df <- subset(dat, zip == zip_list[i])
  
  # exclude the 3 days within any other heatwave
  df$time <- 1:nrow(df)
  cand_control <- unique(c(which(df$heatwave == 1) , which(df$heatwave == 1) + 1,
                           which(df$heatwave == 1) - 1))
  df$cand_control <- TRUE
  df$cand_control[cand_control] <- FALSE
  
  case_dates <- subset(df, heatwave == 1)
  control_dates <- subset(df, cand_control == TRUE) # Modified
  
  for(j in 1:nrow(case_dates)){
    # choose lags (lagged 0 to lagged 7)
    lag_dates <- case_dates[j, ]$date + 0:7
    lag_case <- subset(df, date %in% lag_dates)
    
    # choose 10 comparable unexposed days for each heatwave-exposed day
    control_range <- case_dates[j, ]$doy + -3:3
    control_subset <- subset(control_dates,
                             control_dates$year != case_dates[j, ]$year &
                               doy %in% control_range)
    
    # Check if there are enough controls
    if(nrow(control_subset) < 3){
      next # Skip this iteration if there are not enough controls
    }
    
    controls <- dplyr::sample_n(control_subset, 3)
    
    # choose lagged days for selected unexposed days
    la_con <- c(1:7)
    lag_control <- NULL
    for(p in 1:length(la_con)){
      lag_control_dates <- controls$date + la_con[p]
      lag_control_each <- subset(df, date %in% lag_control_dates & cand_control == TRUE) # Modified
      
      if(nrow(lag_control_each) == 0){
        next # Skip this lag if no valid control days
      }
      
      if(is.null(lag_control)){
        lag_control <- lag_control_each
      }else{
        lag_control <- rbind(lag_control, lag_control_each)
      }
    }
    
    # Combine lag_case, controls, and lagged controls
    j_stratum <- rbind(lag_case, controls, lag_control)
    
    # Dynamically set the 'status' and 'lag' vectors based on actual data size
    num_lag_case <- nrow(lag_case)
    num_controls <- nrow(controls)
    num_lag_control <- nrow(lag_control)
    
    stratum <- paste("stratum", j, sep = ".")
    j_stratum$stratum <- stratum
    
    status <- c(rep("case", num_lag_case), rep("control", num_controls + num_lag_control))
    j_stratum$status <- status
    
    lag <- c(0:7, rep(0, num_controls), rep(c(1:7), each = num_controls))
    j_stratum$lag <- lag[1:nrow(j_stratum)]  # Ensure lag vector matches row count
    
    if(j == 1){
      new_df <- j_stratum
    }else{
      new_df <- rbind(new_df, j_stratum)
    }
  }
  
  if(i == 1){
    matched_df <- new_df
  }else{
    matched_df <- rbind(matched_df, new_df)
  }
}

# matched_df is a matched multi-county data set

matched_df_check <- matched_df %>%
  filter(cand_control == "FALSE" & status == "control")

matched_control <- matched_df %>%
  filter(status == 'control')
table(matched_control$heatwave)

write_parquet(matched_df, "Matched_DF_GDM_HDP_PTB_CSEC_IND.parquet")

table(matched_df$lag)

# This should be an empty dataframe if the code worked correctly :) 

