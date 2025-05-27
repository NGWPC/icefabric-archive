# Table Building Guide

## Overview

IceFabric Manage is a Python package that simplifies building Apache Iceberg catalogs from Parquet files

## Architecture

The system consists of two main components:

1. **Core API** (`src/icefabric_manage/_api.py`) - Contains the table building logic
2. **Build Scripts** (`builds/`) - Orchestrates the catalog creation process

## How It Works

!!! info "Build Process Overview"
    The system automatically converts Parquet files into queryable Iceberg tables through a simple directory scan and conversion process.

### The Build Process

1. **Create Catalog** - Sets up a SQLite-backed Apache Iceberg catalog
2. **Scan Directory** - Finds all `.parquet` files in the specified data directory
3. **Create Tables** - For each Parquet file, creates a corresponding Iceberg table
4. **Populate Data** - Appends the Parquet data to the newly created Iceberg tables

### File Structure

!!! note "File Hierarchy"
    The below files only show the hydrofabric. This logic can be extended to any tabular data managed by Apache Iceberg.

```text
project/
├── src/icefabric_manage/
│   └── _api.py                 # Core table building functions
├── build_hydrofabric.py        # Main build script
└── data/
    ├── warehouse/              # Iceberg catalog storage (auto-created)
    │   └── pyiceberg_catalog.db
    └── hydrofabric/            # Source Parquet files
        ├── divides.parquet
        ├── nexus.parquet
        └── ...
```

## Usage

### Prerequisites

!!! warning "Important Requirements"
    - The `icefabric_manage` package must be installed through `uv sync`
    - There must be parquet files locally to build from. These files are to be downloaded from the NGWPC S3 bucket @ `s3://hydrofabric-data/icefabric/` and placed in `src/icefabric_manage/data/`

### Running the Build

To build a specific table, one must be working/CD into the `scripts/` directory
```bash
python build_hydrofabric.py
```

!!! success "Expected Output"
    ```text
    Created hydrofabric namespace
    Build successful
    ```

### What Happens During Build

1. **Warehouse Setup** - Creates `../data/warehouse/` directory if it doesn't exist
2. **Catalog Configuration** - Sets up SQLite catalog with file-based storage
3. **Namespace Creation** - Creates the `hydrofabric` namespace if it doesn't exist
4. **Table Creation** - For each `.parquet` file in `../data/hydrofabric/`:
    - Uses the filename (without extension) as the table name
    - Reads the Parquet schema and creates matching Iceberg table
    - Appends all data from the Parquet file to the Iceberg table
    - Skips files if tables already exist

## Configuration

### Catalog Settings

The build script uses these default settings:

```python
catalog_settings = {
    "type": "sql",                                              # SQLite-backed catalog
    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",  # Database location
    "warehouse": f"file://{warehouse_path}",                    # Data storage location
}
```

In most functions, it's predefined to the user's `/tmp` dir since it's the same per each user's machine

## Customization

The following customizations can be done to update where/what is being added to the PyIceberg table

### Changing Data Directory

Modify the `data_dir` path in `build_hydrofabric.py`:

```python
data_dir = Path("/path/to/your/parquet/files")
```

### Different Namespace

Change the namespace name:

```python
namespace = "your_namespace"
```
