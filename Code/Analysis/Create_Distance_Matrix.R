
library(tigris)
library(sf)
library(tidyverse)
library(arrow)

# Load shapefile and filter to North Carolina (FIPS 37), reproject 
shp <- block_groups(state = "NC", year = 2010) %>%
  filter(STATEFP == "37")
shp_proj <- st_transform(shp, crs = 32119)
geoids <- shp_proj$GEOID
centroids <- st_geometry(shp_proj)

# Compute pairwise distance matrix
dist_matrix <- st_distance(centroids)
dist_matrix_num <- as.matrix(dist_matrix)
dimnames(dist_matrix_num) <- list(origin = geoids, destination = geoids)

# Convert distance matrix to tidy dataframe
dist_df <- as.data.frame(as.table(dist_matrix_num))
colnames(dist_df) <- c("origin", "destination", "distance_ft")
dist_df <- dist_df %>%
  mutate(distance_miles = as.numeric(distance_ft) / 5280)
gc()

write_parquet(dist_df, "Data/CBG_Dist_Matrix.parquet")