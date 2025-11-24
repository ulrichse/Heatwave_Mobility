### PART I: Processing temperature metrics and defining heatwave events. 

#### HW1_Process_PRISM_Rasters.R
- **INPUT:**  Gridded raster temperature daily metrics from PRISM using `get_prism_dailys()`

- **DATA SOURCE:**  [PRISM](https://prism.oregonstate.edu/recent/)

- **OUTPUT:** 
  - *PRISM_dailys_2012_2024_CBG2020.parquet*
  - *PRISM_dailys_2012_2024_CBG2010.parquet*

- **DESCRIPTION:**
Aggregate gridded PRISM temperature metrics (tmean, tmin, tmax, tdmean) into daily time series at the specified geographic unit level. In this case, the selected geography is the census block group level in North Carolina, both for 2010 (n=6,155) and 2020 (n=~7100). 

#### HW2_Identify_Heatwave_Events.R
- **INPUT:** 
  - *PRISM_dailys_2012_2024_CBG2010.parquet*
  - [NOAA NCEI CONUS Climate Divisions dataset](https://www.ncei.noaa.gov/pub/data/cirs/climdiv/)
 
- **OUTPUT:** 
  - *HW_NC2010CBG_2012_2024.parquet*

- **SOURCE:** 
Code adapted from Luke Wertis ([GitHub Repository](https://github.com/wertisml/Heatwave/tree/main)) and from [Ivan Hanigan] (https://rdrr.io/github/swish-climate-impact-assessment/ExcessHeatIndices/f/README.md)

- **DESCRIPTION:**
First, identify EHF-based heatwave events and second, identify percentile-threshold based heatwave events. 

Process daily time series temperature metrics from PRISM to *identify Excess Heat Factor (EHF) events*. EHF events are defined using the Excessive Heat Index (EHI), which is calculated as the 3-day average temperature in a geographic unit (e.g., census block group), subtracted from the 95th warm season (May to Sept) percentile of heat (over the specified time frame within the source dataset, in this case 2012-2024) for that same geographic unit: 
  - `EHI <- (3 Day temeprature average) - 95th temperature percentile`
  
  `EHI_accl` is the value of the 3-day average subtracted from the average temperature of the previous 30 days in a geographic unit:
    - `EHI_accl <- (3 Day temeprature average) - (previous 30 day temperatre average)`
  
  `EHI_sig` is the variable that signals when the value of the EHI is positive:
    - `EHI_sig <- fifelse(EHI < 0, 0, EHI)`
  
  To calculate EHF, a positive EHI (`EHI_sig`) is multiplied by either 1 or the value of `EHI_accl` if `EHI_accl` is greater than 1. 
    - `EHF <- EHI_sig * max(1, EHI_accl)`
  
  A severe heatwave is defined as a heatwave day where the 3-day average is above the 95th percentile temperature, as well as above the previous 30 days temperature for an area: 
    - `EHF_severe = (ifelse(EHF >= EHF85, 1, 0))`
  
  An extreme heatwave is defined as a heatwave day where the 3-day average is greater than the 85th percentile of all EHF values for that area: 
    - `EHF_extreme = (ifelse(EHF >= (3 * EHF85), 1, 0))`
  
  A low intensity heatwave occurs when the EHF is greater than 0 but less than 1 for the day in an area: 
    - `EHF_low = (ifelse(EHF > 0 & EHF < 1, 1, 0))`
  
  A moderate intensity heatwave occurs when the EHF for that day is greater than or equal to 1 but less than 2: 
    - `EHF_moderate = (ifelse(EHF >= 1 & EHF < 2, 1, 0))`
  
  A high intensity heatwave occurs when the EHF for that day is greater than or equal to 2: 
    - `EHF_high = (ifelse(EHF >= 2, 1, 0))`

Percentile thresholds used to define heatwave events are derived at the climate region level. Climate region data is from the NOAA Monthly U.S. Climate Divisional Database. There are eight such climate regions across NC (NOAA). The use of regionally derived thresholds rather than fixed values or thresholds calculated across the entire state better accounts for local acclimatization to climate conditions [Grundstein & Dowd, 2011](https://journals.ametsoc.org/view/journals/apme/50/8/jamc-d-11-063.1.xml).  Geographic units (e.g. census tract, ZCTAs, or census block groups) are grouped by their corresponding climate division, and temperature thresholds are calculated based on daily average, minimum, and maximum temperature values across all geographic units within the corresponding climate region for the specified time period.  

   The 90th, 95th, and 99th percentile thresholds were calculated for each climate region for average, minimum, and maximum temperature values. The resulting values are shown in the columns `tmean_pct_90`, `tmin_pct_90`, `tmax_pct_90` thru `tmax_pct_99`. The value of `tmean_pct_90`for instance) for a given location reflects the 90th percentile average temperature threshold for the corresponding climate region that a geographic unit is located within.  

  `above_tmean_pct_90`, `above_tmin_pct_90`, `above_tmax_pct_90` thru `above_tmax_pct_99`, denote whether the average (tmean), minimum (tmin), or maximum (tmean) temperature for each geographic unit for each day during the time period was above the corresponding temperature threshold for that climate region. A value of ‘1’ for `above_tmean_pct_95` for a geographic unit on a given day means the daily average temperature was in excess of the corresponding 95th percentile temperature threshold (`tmean_pct_95`) value for that geographic unit.  

  Heatwave events are defined based on incidences of consecutive days above a specified percentile threshold. This analysis specifies a 2, 3, and 4-day duration of heat events above the 90th, 95th, and 99th percentiles for average, minimum, and maximum temperatures. The final day of a given heatwave event is denoted with a ‘1’ in the corresponding column (ex. above_tmean_pct_95_two_days).  

  Bivariate heatwave events are defined as 2, 3, or 4 consecutive days where both the minimum and maximum daily temperature was above the respective minimum or maximum temperature threshold for that census block group (ex. above_tmin_tmax_pct_90_two_days).  

### PART II: Processing cell-phone-based mobility data

#### M1_Process_Weekly_Summary.R

- **INPUT:**
  - Advan/SafeGraph weekly patterns 
  - Advan/SafeGraph weekly patterns POI information
    
- **REFERENCE:**
  - [NAICS Codebook](https://www.census.gov/naics/?58967?yearbck=2022)

- **OUTPUT:** 
  - *Weekly_Patterns_POI_2022_2024.parquet*

- **DESCRIPTION:**
This script merges North Carolina SafeGraph/Advan weekly raw visitor and visit counts (2022-2024) to each Point of Interest (POI) with POI metadata (NAICS code, NAICS top category, NAICS subcategory). 

### PART III: Merging mobility and heatwave datasets

#### M2_Merge_Weekly_Visits_EHF.R

- **INPUT:**
  - *Weekly_Patterns_POI_2022_2024.parquet*
  - *EHF_Heatwave_NC2010CBG_2012_2024.parquet*
    
- **OUTPUT:** 
  - *Weekly_Series_Visits_EHF_Merge_2022_2024.parquet*

- **DESCRIPTION:**
This script aggregates the total visitors and visits to POIs by week and NAICS code for each census block group, and aggregates the number of EHF-based heatwave events and their intensities from the daily to the weekly level. The weekly mobility and heatwave datasets are merged at the census block group level. 

### PART IV: Analysis

#### Create_Pop_File.R
- **INPUT:**
  - [NHGIS 2020 to 2010 CBG Crosswalk File](https://www.nhgis.org/geographic-crosswalks#to-block-groups)
    
- **OUTPUT:** 
  - *annual_acs_population_2012_2024_2010cbg.csv*

- **DESCRIPTION:**
- Uses `tidycensus` package to create annual population counts at 2010 census block group geographies for 2012 to 2024. 













