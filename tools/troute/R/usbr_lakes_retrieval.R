# --- 0. Install and load necessary packages ---
gpkg_path <- "conus_nextgen.gpkg"
required_packages <- c("httr", "jsonlite", "dplyr", "sf", "stringdist")
installed <- rownames(installed.packages())
for (pkg in required_packages) {
  if (!(pkg %in% installed)) install.packages(pkg)
}
lapply(required_packages, library, character.only = TRUE)

# --- 1. Retrieve all reservoir locations from RISE API (handle pagination) ---
base_url <- "https://data.usbr.gov/rise/api/location"
all_data <- list()
page <- 1
repeat {
  resp <- GET(
    url = base_url,
    query = list(page = page, itemsPerPage = 100),
    add_headers(Accept = "application/vnd.api+json")
  )
  stop_for_status(resp)

  content_page <- fromJSON(content(resp, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
  data_page <- content_page$data

  if (length(data_page) == 0) break  # Exit loop if no more data

  all_data <- c(all_data, data_page)
  page <- page + 1
}

cat("Total entries retrieved:", length(all_data), "\n")
all_locations <- lapply(all_data, function(x) {
  attrs <- x$attributes
  coords <- attrs$locationCoordinates$coordinates
  if (is.null(coords) || length(coords) != 2) return(NULL)

  data.frame(
    locationName = attrs$locationName,
    longitude = coords[[1]],
    latitude = coords[[2]],
    locationType = attrs$locationTypeName,
    stringsAsFactors = FALSE
  )
})

# Check locationTypes
all_types <- sapply(all_data, function(x) x$attributes$locationTypeName)
print(sort(table(all_types), decreasing = TRUE))
all_locations_df <- bind_rows(Filter(Negate(is.null), all_locations))

# Optional: re-check location types in geolocated entries
location_type_counts <- sort(table(all_locations_df$locationType), decreasing = TRUE)
print(location_type_counts)

# Filter for "Lake/Reservoir" only (or expand this list if desired)
reservoir_df <- all_locations_df %>%
  filter(locationType == "Lake/Reservoir")

# Convert to sf object
reservoir_sf <- st_as_sf(reservoir_df, coords = c("longitude", "latitude"), crs = 4326)

# --- 4. Load lakes layer from geopackage ---
lakes_layer_name <- "lakes"
lakes_sf <- st_read(gpkg_path, layer = lakes_layer_name, quiet = TRUE)

# Ensure matching CRS
if (st_crs(lakes_sf) != st_crs(reservoir_sf)) {
  lakes_sf <- st_transform(lakes_sf, st_crs(reservoir_sf))
}

# --- 5. Find lakes within buffer distance of each reservoir ---
buffer_dist_meters <- 7500  # 7.5 km buffer
reservoir_buffer <- st_buffer(reservoir_sf, dist = buffer_dist_meters)

# Find intersecting lakes
nearby_matches <- st_join(reservoir_buffer, lakes_sf, join = st_intersects, left = FALSE)

# --- 6. Fuzzy string match lake names to reservoir names ---
if ("conus_name" %in% names(nearby_matches)) {
  nearby_matches <- nearby_matches %>%
    rowwise() %>%
    mutate(
      fuzzy_score = stringdist::stringdist(locationName, conus_name, method = "jw"),
      fuzzy_match = conus_name
    ) %>%
    ungroup()
}

# --- 7. Output summary ---
cat("Total geolocated entries:", nrow(all_locations_df), "\n")
cat("Total 'Lake/Reservoir' entries:", nrow(reservoir_sf), "\n")
cat("Total lakes loaded:", nrow(lakes_sf), "\n")
cat("Number of reservoirs near lakes (within 5km):", nrow(nearby_matches), "\n\n")

# Preview matched features
print(head(nearby_matches))

# --- 8. Export matched results to geopackages ---
# Create output geopackage with matched reservoirs and lakes (with geometry)
output_gpkg <- "matched_reservoirs_lakes.gpkg"
st_write(nearby_matches, output_gpkg, layer = "matched_reservoirs_lakes", delete_dsn = TRUE)

# --- 9. Export simplified match table to geopackage ---
# Create a simple match table as a non-spatial layer in the same geopackage
simple_match_df <- nearby_matches %>%
  st_drop_geometry() %>%
  select(locationName, lake_id) %>%
  distinct()

# Write the simple match table as a non-spatial layer to the same geopackage
st_write(simple_match_df, output_gpkg, layer = "reservoir_lake_matches", append = TRUE)

cat("Output written to:", output_gpkg, "\n")
cat("Layers created:\n")
cat("  - matched_reservoirs_lakes (spatial layer with geometry)\n")
cat("  - reservoir_lake_matches (non-spatial lookup table)\n")
