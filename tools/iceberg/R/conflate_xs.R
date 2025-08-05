
#!/usr/bin/env Rscript

# FEMA BLE and MIP Cross-Section Processing Script - ALL SUBMODELS
# Author: Lynker-Spatial, Raytheon
# Converts FEMA flood model cross-sections to representative channel geometry features

# NOTE THIS FILE REQUIRES THE FOLLOWING LIBRARIES: dplyr, sf, and glue. This file also requires an installation of R

# Set your file paths here
ref_path <- "./reference_fabric.gpkg"
fema <- "./data/mip_full_collection"
output_file <- "./all_processed_cross_sections.gpkg"

# Load required packages
library(dplyr)
library(sf)
library(glue)

## 1. Elevation Smoothing Utility
clean_elev <- function(elev_vec, threshold = 100) {
  for (i in which(elev_vec > threshold)) {
    if (i > 1 && i < length(elev_vec)) {
      elev_vec[i] <- mean(c(elev_vec[i - 1], elev_vec[i + 1]), na.rm = TRUE)
    } else if (i == 1) {
      elev_vec[i] <- elev_vec[i + 1]
    } else if (i == length(elev_vec)) {
      elev_vec[i] <- elev_vec[i - 1]
    }
  }
  elev_vec
}

## 2. Channel Area Calculation
.findCA <- function(df, depth) {
  Y <- NULL
  t <- filter(df, Y <= depth)
  suppressWarnings({
    x <- pmax(
      0,
      DescTools::AUC(x = t$x, y = rep(depth, nrow(t)), absolutearea = FALSE) -
        DescTools::AUC(x = t$x, y = t$Y, absolutearea = FALSE)
    )
  })
  ifelse(is.na(x), 0, x)
}

# Find all BLE and MIP model directories
message("Discovering BLE and MIP model directories...")
all_dirs <- list.dirs(fema, recursive = FALSE, full.names = FALSE)
ble_dirs <- all_dirs[grepl("^ble_", all_dirs)]
mip_dirs <- all_dirs[grepl("^mip_", all_dirs)]
model_dirs <- c(ble_dirs, mip_dirs)
message("Found ", length(ble_dirs), " BLE model directories")
message("Found ", length(mip_dirs), " MIP model directories")
message("Total: ", length(model_dirs), " model directories to process")

# Initialize storage for all results
all_subs_data <- list()
ble_counter <- 0

# Process each BLE and MIP model directory
for (model_dir in model_dirs) {
  ble_counter <- ble_counter + 1
  message("\n=== Processing Model ", ble_counter, "/", length(model_dirs), ": ", model_dir, " ===")

  # Set up paths for this model
  submodel_dir <- glue::glue("{fema}/{model_dir}/submodels/")

  # Check if submodels directory exists
  if (!dir.exists(submodel_dir)) {
    message("  Skipping - no submodels directory found")
    next
  }

  # Discover submodel files for this BLE model
  subs <- list.files(submodel_dir, recursive = TRUE, pattern = ".gpkg$", full.names = TRUE) |>
    as.data.frame() |>
    setNames("file") |>
    mutate(
      reach = gsub('.*/', '', file),
      reach = gsub('.gpkg', '', reach),
      name = model_dir  # Use the model name directly
    )

  if (nrow(subs) == 0) {
    message("  Skipping - no .gpkg files found in submodels")
    next
  }

  message("  Found ", nrow(subs), " submodel files")

  # Process each submodel for this BLE model
  subs_data <- list()

  for (v in 1:nrow(subs)) {
    if (v %% 10 == 0) {  # Progress update every 10 files
      message("    Processing file ", v, "/", nrow(subs))
    }

    tryCatch({
      # Check if XS layer exists
      layers <- st_layers(subs$file[v])$name
      if (!"XS" %in% layers) {
        next  # Skip files without XS layer
      }

      # Read XS layer from submodel and transform to projected CRS
      transects <- st_transform(read_sf(subs$file[v], 'XS'), 5070)
      ll <- list()

      for (j in 1:nrow(transects)) {
        # Parse and clean station-elevation profile
        cleaned <- gsub("\\[|\\]|\\(|\\)", "", transects$station_elevation_points[j])
        cleaned <- strsplit(cleaned, ", ")[[1]]
        df <- as.data.frame(matrix(as.numeric(cleaned), ncol = 2, byrow = TRUE))
        names(df) <- c("x", "Y")

        # Extract left/right bank stations
        pins <- transects$bank_stations[j] %>%
          gsub("\\[|\\]|\\'", "", .) |>
          strsplit(",\\s*") |>
          unlist() |>
          as.numeric()

        # Subset profile to only between banks and clean elevation
        result <- dplyr::filter(df, dplyr::between(x, pins[1], pins[2]))
        result$Y <- clean_elev(result$Y)

        if (nrow(result) <= 2 | diff(range(result$Y)) < .25) {
          # Skip degenerate transects
          next
        } else {
          # Compute channel geometry attributes
          result$Ym <- max(result$Y) - min(result$Y)  # channel depth
          result$TW <- max(result$x) - min(result$x)  # top width
          result$flowpath_id <- subs$reach[v]
          result$river_station <- transects$river_station[j]
          result$model = subs$file[v]
          result$A <- .findCA(result, max(result$Y))  # channel area
          result$r <- result$A / ((result$Ym * result$TW) - result$A)  # Dingmans R shape
          result$domain <- subs$name[v]

          # Join metadata and convert to spatial
          ll[[j]] <- dplyr::distinct(dplyr::select(result, -x, -Y)) |>
            slice(1) |>
            left_join(
              select(transects[j,],
                     river_station, river_reach_rs,
                     source_river, source_reach, source_river_station,
                     station_elevation_points, bank_stations),
              by = c('river_station')
            ) |>
            st_as_sf()
        }
      }

      # Try reading metadata layer for units
      meta = suppressWarnings({
        tryCatch({
          read_sf(subs$file[v], 'metadata') |> filter(key == "units")
        }, error = function(e) {
          data.frame(value = NA)
        })
      })

      # Create CRS and units metadata
      meta = meta |>
        mutate(flowpath_id = subs$reach[v],
               epsg = st_crs(read_sf(subs$file[v], 'XS'))$epsg,
               crs_units = st_crs(read_sf(subs$file[v], 'XS'))$units) |>
        select(flowpath_id, metdata_units = value, epsg, crs_units)

      fin = bind_rows(ll)

      # Store only if valid data
      if(nrow(fin) > 0 & nrow(meta) > 0) {
        subs_data[[v]] <- left_join(fin, meta, by = "flowpath_id")
      }

    }, error = function(e) {
      # Log errors but continue processing
      message("    Error processing ", basename(subs$file[v]), ": ", e$message)
    })
  }

  # Combine data for this BLE model
  if (length(subs_data) > 0) {
    ble_huc_xs <- tibble(data.table::rbindlist(subs_data)) |>
      st_as_sf()

    message("  Processed ", nrow(ble_huc_xs), " cross-sections from ", model_dir)

    # Store in master list
    all_subs_data[[model_dir]] <- ble_huc_xs
  } else {
    message("  No valid data found for ", model_dir)
  }
}

# Combine all BLE and MIP model data
message("\n=== Combining all model data ===")
if (length(all_subs_data) > 0) {
  all_huc_xs <- bind_rows(all_subs_data) |>
    st_as_sf()

  message("Total cross-sections processed: ", nrow(all_huc_xs))

  # Compute representative XS features per flowpath
  message("Computing representative features...")
  representative_features <- all_huc_xs |>
    tidyr::drop_na(flowpath_id) |>
    dplyr::group_by(flowpath_id) |>
    arrange(river_station) |>
    dplyr::summarise(
      r  = mean(r[is.finite(r)]),
      TW = mean(TW),
      Y  = mean(Ym),
      geom = geom[ceiling(n()/2)],
      source_river_station = source_river_station[ceiling(n()/2)],
      river_station = river_station[ceiling(n()/2)],
      model = model[ceiling(n()/2)],
      .groups = 'drop')

  # Get NHD metadata
  message("Downloading NHD metadata...")
  nhd_meta <- nhdplusTools::get_vaa(c('ftype', 'streamorde')) |>
    mutate(comid = as.character(comid))

  # Final output
  out_xs <- representative_features |>
    left_join(nhd_meta, by = c('flowpath_id' = 'comid'))

  # Save results
  message("Saving results...")
  write_sf(out_xs, output_file)

  message("=== PROCESSING COMPLETE ===")
  message("Final output: ", nrow(out_xs), " representative cross-sections")
  message("Saved to: ", output_file)

  glimpse(out_xs)

} else {
  message("No valid data found in any model directories")
}
