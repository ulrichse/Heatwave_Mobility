### PART I: Identify Heatwaves

<details>
  <summary><strong> HW1_Process_PRISM_Rasters.R </strong></summary>
  
- **INPUT:**  Gridded raster temperature daily metrics from PRISM using `get_prism_dailys()`

- **DATA SOURCE:**  [PRISM](https://prism.oregonstate.edu/recent/)

- **OUTPUT:** 
  - *PRISM_Dailys_2012_2024_CBG2010*

- **DESCRIPTION:**
Aggregate gridded PRISM temperature metrics (tmean, tmin, tmax, tdmean) into daily time series at the specified geographic unit level. In this case, the selected geography is the census block group level in North Carolina (2010 boundaries, n=6,155). 
</details>
<details>
<summary><strong> HW2_Identify_Heatwaves.R </strong></summary>
  
- **INPUT:** 
  - *PRISM_Dailys_2012_2024_CBG2010.parquet*
  - [NOAA NCEI CONUS Climate Divisions dataset](https://www.ncei.noaa.gov/pub/data/cirs/climdiv/)
 
- **OUTPUT:** 
  - *NC_CBG2010_Heatwaves.parquet*

- **SOURCE:** 
Code adapted from Luke Wertis ([GitHub](https://github.com/wertisml/Heatwave/tree/main)) and from Ivan Hanigan [(GitHub)](https://rdrr.io/github/swish-climate-impact-assessment/ExcessHeatIndices/f/README.md)

- **DESCRIPTION:**
<details>
<summary> 1) Identify EHF-based events</summary>


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
</details>

<details>
  <summary> 2) Identify percentile threshold-based events </summary>
  

Percentile thresholds used to define heatwave events are derived at the climate region level. Climate region data is from the NOAA Monthly U.S. Climate Divisional Database. There are eight such climate regions across NC (NOAA). The use of regionally derived thresholds rather than fixed values or thresholds calculated across the entire state better accounts for local acclimatization to climate conditions [Grundstein & Dowd, 2011](https://journals.ametsoc.org/view/journals/apme/50/8/jamc-d-11-063.1.xml).  Geographic units (e.g. census tract, ZCTAs, or census block groups) are grouped by their corresponding climate division, and temperature thresholds are calculated based on daily average, minimum, and maximum temperature values across all geographic units within the corresponding climate region for the specified time period.  

   The 90th, 95th, and 99th percentile thresholds were calculated for each climate region for average, minimum, and maximum temperature values. The resulting values are shown in the columns `tmean_pct_90`, `tmin_pct_90`, `tmax_pct_90` thru `tmax_pct_99`. The value of `tmean_pct_90`for instance) for a given location reflects the 90th percentile average temperature threshold for the corresponding climate region that a geographic unit is located within.  

  `above_tmean_pct_90`, `above_tmin_pct_90`, `above_tmax_pct_90` thru `above_tmax_pct_99`, denote whether the average (tmean), minimum (tmin), or maximum (tmean) temperature for each geographic unit for each day during the time period was above the corresponding temperature threshold for that climate region. A value of ‘1’ for `above_tmean_pct_95` for a geographic unit on a given day means the daily average temperature was in excess of the corresponding 95th percentile temperature threshold (`tmean_pct_95`) value for that geographic unit.  

  Heatwave events are defined based on incidences of consecutive days above a specified percentile threshold. This analysis specifies a 2, 3, and 4-day duration of heat events above the 90th, 95th, and 99th percentiles for average, minimum, and maximum temperatures. The final day of a given heatwave event is denoted with a ‘1’ in the corresponding column (ex. above_tmean_pct_95_two_days).  

  Bivariate heatwave events are defined as 2, 3, or 4 consecutive days where both the minimum and maximum daily temperature was above the respective minimum or maximum temperature threshold for that census block group (ex. above_tmin_tmax_pct_90_two_days).  
</details>

</details>

### PART II: Process Advan/Safegraph Weekly Patterns

<details>
  <summary><strong> M1_POI_Info.R </strong></summary>
- **INPUT:**
  - Advan/SafeGraph Weekly Patterns *(NC_weeklypatterns_poi_info_plus_)*
    
- **REFERENCE:**
  - [NAICS Codebook](https://www.census.gov/naics/?58967?yearbck=2022)

- **OUTPUT:** 
  - *POI_Info_Plus_2022_2024.parquet*

- **DESCRIPTION:**
This script merges North Carolina SafeGraph/Advan weekly POI metadata (NAICS code, NAICS top category, NAICS subcategory) into a single file.

</details>

<details>
<summary><strong>M2_Weekly_POI_Visits.R</strong></summary> 

- **INPUT:**
  - Advan/SafeGraph Weekly Patterns *(NC_weeklypatterns_summary)*
  - *POI_Info_Plus_2022_2024.parquet*

- **OUTPUT:** 
  - *Weekly_POI_Visits_2022_2024.parquet*

- **DESCRIPTION:**
This script merges North Carolina SafeGraph/Advan weekly POI visitor and visit counts (2022-2024) into a single file with POI metadata (NAICS code, NAICS top category, NAICS subcategory).
</details>

<details>
<summary><strong>M3_Weekly_CBG_Trips.R</strong></summary> 

- **INPUT:**
  - Advan/SafeGraph Weekly Patterns *(NC_weeklypattens_od_home)*
  - *CBG2010_Database.csv* (Contains 2019 population count, NCEI climate division, RUCA category, and physiographic region for each CBG)
  - *POI_Info_Plus_2022_2024.parquet*

- **OUTPUT:** 
  - *Weekly_CBG_Trips_2022_2024.parquet*

- **DESCRIPTION:**
Construct a weekly time series for each CBG in NC with the total number of trips taken outside of the home CBG, as well as trip counts stratified by distance. Uses destination/POI CBG from the POI metadata file and constructs a CBG distance matrix to create travel distance categories. Includes attribute information for each CBG. 
</details>

<details>
 <summary><strong>M4_Daily_POI_Visits.R</strong></summary>

- **INPUT:**
  - Advan/SafeGraph Weekly Patterns *(NC_weeklypatterns_daily)*
  - *POI_Info_Plus_2022_2024.parquet*

- **OUTPUT:** 
  - *Daily_Patterns_POI_2022_2024.parquet*

- **DESCRIPTION:**
Pivot weekly visit and visitor counts for each POI into a daily time series. 
</details>

### PART III: Analysis & Models

<details>
<summary><strong>A1_Test_Origin_Models.R</strong></summary>

- **INPUT:**
  - *Weekly_Away_Visits_2022_2024.parquet*
  - *NC_CBG2010_Heatwaves.parquet*
    
- **OUTPUT:** 
  - Model results

- **DESCRIPTION:**
Predict weekly quantity of away visits based on number of days in a week under a heat regime, for each heatwave definition. Aggregates heat metrics to weekly time series, analysis restricted to warm season. 
</details>

### PART IV: Additional File Creation

<details>
<summary><strong>Create_Pop_File.R</strong></summary>
  
- **INPUT:**
  - [NHGIS 2020 to 2010 CBG Crosswalk File](https://www.nhgis.org/geographic-crosswalks#to-block-groups)
    
- **OUTPUT:** 
  - *annual_acs_population_2012_2024_2010cbg.csv*

- **DESCRIPTION:**
- Uses `tidycensus` package to create annual population counts at 2010 census block group geographies for 2012 to 2024. 
</details>

<details>
<summary><strong>Create_CBG_Database.R</strong></summary>
  
- **INPUT:**
  - [NOAA NCEI CONUS Climate Divisions dataset](https://www.ncei.noaa.gov/pub/data/cirs/climdiv/)
  - [RUCA County Codes](https://www.ers.usda.gov/data-products/rural-urban-continuum-codesRuralurbancontinuumcodes2023.csv)
    
- **OUTPUT:** 
  - *CBG2010_Database.csv*

- **DESCRIPTION:**
- Output file indicates rural-urban and physiographic region for each CBG. 
</details>

<details>
<summary><strong>Create_Distance_Matrix.R</strong></summary>
  
- **INPUT:**
  - Census block group boundaries from the `tigris` package
    
- **OUTPUT:** 
  - *CBG_Dist_Matrix.parquet*

- **DESCRIPTION:**
- Calculate distance between the centroids of every CBG in NC.
</details>












