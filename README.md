### PART I: Processing temperature metrics and defining heatwave events. 

#### 1_Process_PRISM_Rasters.R
- **INPUT:**  Gridded raster temperature daily metrics from PRISM using get_prism_dailys()

- **DATA SOURCE:**  ([PRISM] (https://prism.oregonstate.edu/recent/))

- **OUTPUT:** 
  - PRISM_dailys_2012_2024_CBG2020.parquet
  - PRISM_dailys_2012_2024_CBG2010.parquet

- **DESCRIPTION:**
Aggregate gridded PRISM temperature metrics (tmean, tmin, tmax, tdmean) into daily time series at the specified geographic unit level. In this case, the selected geography is the census block group level in North Carolina, both for 2010 (n=6,155) and 2020 (n=~7100). 

#### 2_Identify_EHF_Events.R
- **INPUT:** 
  - PRISM_dailys_2012_2024_CBG2020.parquet
  - PRISM_dailys_2012_2024_CBG2010.parquet
 
- **OUTPUT:** 
  - EHF_Heatwave_NC2020CBG_2012_2024.parquet
  - EHF_Heatwave_NC2010CBG_2012_2024.parquet

- **SOURCE:** 
Code adapted from Luke Wertis ([GitHub Repository](https://github.com/wertisml/Heatwave/tree/main))

- **DESCRIPTION:**

Process daily time series temperature metrics from PRISM to identify Excess Heat Factor (EHF) events. EHF events are defined using the Excessive Heat Index (EHI), which is calculated as the 3-day average temperature in a geographic unit (e.g., census block group), subtracted from the 95th percentile of heat (over the specified time frame within the source dataset, in this case 2012-2024) for that same geographic unit: 
  - `EHI <- (3 Day temeprature average) - 95th temperature percentile`
  
`EHI_accl` is the value of the 3-day average subtracted from the average temperature of the previous 30 days in a geographic unit:
  - `EHI_accl <- (3 Day temeprature average) - (previous 30 day temperatre average)`
  
`EHI_sig` is the variable that signals when the value of the EHI is positive:
  - `EHI_sig <- fifelse(EHI < 0, 0, EHI)`
  
To calculate EHF, a positive EHI (`EHI_sig`) is multiplied by either 1 or the value of `EHI_accl` if `EHI_accl` is greater than 1. 
  - `EHF <- EHI_sig * max(1, EHI_accl)`
  
The variable `No_Heatwave` identified days that were not heatwaves. This is calculated by subtracting `Heatwave_condition` (which is 1 for heatwave days and 0 otherwise) from daynum (which is always 1). If the result is 1, the day was not a heatwave:
  - `No_Heatwave = (daynum - Heatwave_condition)`
  
The `Heatwave_condition` column is used to indicate the severity of a heatwave.
A severe heatwave is defined as a heatwave day where the 3-day average is above the 95th percentile temperature, as well as above the previous 30 days temperature for an area: 
  - `Severe_Heatwaves = (ifelse(EHF >= EHF85, 1, 0))`
  
An extreme heatwave is defined as a heatwave day where the 3-day average is greater than the 85th percentile of all EHF values for that area: 
  - `Extreme_Heatwaves = (ifelse(EHF >= (3 * EHF85), 1, 0))`
  
A low intensity heatwave occurs when the EHF is greater than 0 but less than 1 for the day in an area: 
  - `low_intensity = (ifelse(EHF > 0 & EHF < 1, 1, 0))`
  
A moderate intensity heatwave occurs when the EHF for that day is greater than or equal to 1 but less than 2: 
  - `moderate_intensity = (ifelse(EHF >= 1 & EHF < 2, 1, 0))`
  
A high intensity heatwave occurs when the EHF for that day is greater than or equal to 2: 
  - `high_intensity = (ifelse(EHF >= 2, 1, 0))`

#### 3_Identify_Heatwaves.R

### PART II: Processing cell-phone-based mobility data



### PART III: Merging mobility and heatwave datasets

