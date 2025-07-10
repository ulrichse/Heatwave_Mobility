### PART I: Processing temperature metrics and defining heatwave events. 

#### HW1_Process_PRISM_Rasters.R
- **INPUT:**  Gridded raster temperature daily metrics from PRISM using `get_prism_dailys()`

- **DATA SOURCE:**  [PRISM](https://prism.oregonstate.edu/recent/)

- **OUTPUT:** 
  - *PRISM_dailys_2012_2024_CBG2020.parquet*
  - *PRISM_dailys_2012_2024_CBG2010.parquet*

- **DESCRIPTION:**
Aggregate gridded PRISM temperature metrics (tmean, tmin, tmax, tdmean) into daily time series at the specified geographic unit level. In this case, the selected geography is the census block group level in North Carolina, both for 2010 (n=6,155) and 2020 (n=~7100). 

#### HW2_Identify_EHF_Events.R
- **INPUT:** 
  - *PRISM_dailys_2012_2024_CBG2020.parquet*
  - *PRISM_dailys_2012_2024_CBG2010.parquet*
 
- **OUTPUT:** 
  - *EHF_Heatwave_NC2020CBG_2012_2024.parquet*
  - *EHF_Heatwave_NC2010CBG_2012_2024.parquet*

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

#### HW3_Identify_Heatwaves.R

- **INPUT:**
  - [NOAA NCEI CONUS Climate Divisions dataset](https://www.ncei.noaa.gov/pub/data/cirs/climdiv/)
  - *PRISM_dailys_2012_2024_CBG2010.parquet*
  - *PRISM_dailys_2010_2024_CBG2020.parquet*

- **OUTPUT:** 
  - *NC_CBG2010_Heatwaves.parquet*
  - *NC_CBG2020_Heatwaves.parquet*

- **DESCRIPTION:**
Percentile thresholds used to define heatwave events are derived at the climate region level. Climate region data is from the NOAA Monthly U.S. Climate Divisional Database. There are eight such climate regions across NC (NOAA). The use of regionally derived thresholds rather than fixed values or thresholds calculated across the entire state better accounts for local acclimatization to climate conditions [Grundstein & Dowd, 2011](https://journals.ametsoc.org/view/journals/apme/50/8/jamc-d-11-063.1.xml).  Geographic units (e.g. census tract, ZCTAs, or census block groups) are grouped by their corresponding climate division, and temperature thresholds are calculated based on daily average, minimum, and maximum temperature values across all geographic units within the corresponding climate region for the specified time period.  

   The 75th, 85th, 90th, 95th, 97.5th, and 99th percentile thresholds were calculated for each climate region for average, minimum, and maximum temperature values. The resulting values are shown in the columns `tmean_pct_75`, `tmin_pct_75`, `tmax_pct_75` thru `tmax_pct_99`. The value of `tmean_pct_90`for instance) for a given location reflects the 90th percentile average temperature threshold for the corresponding climate region that a geographic unit is located within.  

  `above_tmean_pct_75`, `above_tmin_pct_75`, `above_tmax_pct_75` thru `above_tmax_pct_99`, denote whether the average (tmean), minimum (tmin), or maximum (tmean) temperature for each geographic unit for each day during the time period was above the corresponding temperature threshold for that climate region. A value of ‘1’ for `above_tmean_pct_95` for a geographic unit on a given day means the daily average temperature was in excess of the corresponding 95th percentile temperature threshold (`tmean_pct_95`) value for that geographic unit.  

  Heatwave events are defined based on incidences of consecutive days above a specified percentile threshold. This analysis specifies a 2, 3, and 4-day duration of heat events above the 75th, 85th, 90th, 95th, 97.5th, and 99th percentiles for average, minimum, and maximum temperatures. The final day of a given heatwave event is denoted with a ‘1’ in the corresponding column (ex. above_tmean_pct_75_two_days).  

  Bivariate heatwave events are defined as 2, 3, or 4 consecutive days where both the minimum and maximum daily temperature was above the respective minimum or maximum temperature threshold for that ZCTA (ex. above_tmin_tmax_pct_90_two_days).  

### PART II: Processing cell-phone-based mobility data

#### M1_Process_POI_Categories_Weekly.R

- **INPUT:**
  - Advan/SafeGraph weekly patterns 
  - Advan/SafeGraph weekly patterns POI information
    
- **REFERENCE:**
  - [NAICS Codebook](https://www.census.gov/naics/?58967?yearbck=2022)

- **OUTPUT:** 
  - *Weekly_Patterns_POI_2022_2024.parquet*

- **DESCRIPTION:**
This script processes North Carolina SafeGraph (Advan) Weekly Patterns data to categorize Points of Interest (POIs) by industry using NAICS codes. It reads in POI metadata and assigns each location to a standardized industry category based on NAICS codes (e.g., Retail, Healthcare, Accommodation) and also classifies POIs into higher-level groupings (e.g., "Anchor" institutions like manufacturing or utilities). Weekly visitation summaries are merged with the categorized POI metadata. 

### PART III: Merging mobility and heatwave datasets

#### M2_Merge_Weekly_Visits_EHF.R

- **INPUT:**
  - *Weekly_Patterns_POI_2022_2024.parquet*
  - *EHF_Heatwave_NC2010CBG_2012_2024.parquet*
    
- **OUTPUT:** 
  - *Weekly_Series_Visits_EHF_Merge_2022_2024.parquet*

- **DESCRIPTION:**
This script aggregates the total visitors and visits to POIs by week and NAICS code for each census block group, and aggregates the number of EHF-based heatwave events and their intensities from the daily to the weekly level. The weekly mobility and heatwave datasets are merged at the census block group level. 















