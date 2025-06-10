# Table Building Guide

## Overview

IceFabric Manage is a Python package that simplifies building Apache Iceberg catalogs from Parquet files stored on S3 or locally.

## Architecture

The system consists of two main components from within the `icefabric/src/icefabric_manage/` structure:

1. **Source Code** (`src/icefabric_manage/`) - Contains the table building logic and objects
2. **Build Scripts** (`builds/`) - Orchestrates the catalog creation process from the S3 Tables

## How It Works

!!! info "Build Process Overview"
    The system converts local Parquet file into queryable Iceberg tables through a simple file-by-file conversion process.

### The Build Process

1. **Create Namespace** - Creates the specified namespace if it doesn't exist
2. **Create Tables** - For each specified Parquet file, creates a corresponding Iceberg table
3. **Populate Data** - Appends the Parquet data to the newly created Iceberg tables

See `builds/README.md` for a template on how to create your own build script

## Usage

### Prerequisites

!!! warning "Important Requirements"
    - The `icefabric_manage` package must be installed through `uv sync`
    - AWS credentials configured for S3 access (if using S3 files)
    - Environment variables loaded via `.env` file

### Running the Build Scripts

Build scripts have to be only run once as the files will live/persist in S3.

To build the hydrofabric dataset, run from the `scripts/` directory:
```bash
python build_hydrofabric.py
```

To build USGS streamflow observations:
```bash
python builds/build_hydrofabric.py
```

!!! success "Expected Output"
    ```text
    building layer: divide-attributes
    building layer: divides
    building layer: flowpath-attributes-ml
    ...
    Build successful
    ```
