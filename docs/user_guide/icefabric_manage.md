# Table Building Guide

## Overview

IceFabric Manage is a Python package that simplifies building Apache Iceberg catalogs from Parquet files stored on S3 or locally.

## Architecture

The system consists of two main components:

1. **Core API** (`src/icefabric_manage/_api.py`) - Contains the table building logic
2. **Build Scripts** (`builds/`) - Orchestrates the catalog creation process

## How It Works

!!! info "Build Process Overview"
    The system converts individual Parquet files (local or S3) into queryable Iceberg tables through a simple file-by-file conversion process.

### The Build Process

1. **Create Catalog** - Sets up a SQLite-backed Apache Iceberg catalog in `/tmp/warehouse`
2. **Create Namespace** - Creates the specified namespace if it doesn't exist
3. **Create Tables** - For each specified Parquet file, creates a corresponding Iceberg table
4. **Populate Data** - Appends the Parquet data to the newly created Iceberg tables

### File Structure

!!! note "Project Structure"
    The warehouse is now stored in `/tmp/warehouse` and supports both S3 and local Parquet files.

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
    - AWS credentials configured for S3 access (if using S3 files)
    - Environment variables loaded via `.env` file

### Running the Build Scripts

To build the hydrofabric dataset, run from the `scripts/` directory:
```bash
python build_hydrofabric.py
```

To build USGS streamflow observations:
```bash
python build_usgs_streamflow_observations.py
```

!!! success "Expected Output"
    ```text
    building layer: divide-attributes
    building layer: divides
    building layer: flowpath-attributes-ml
    ...
    Build successful
    ```

### What Happens During Build

1. **Warehouse Setup** - Creates `/tmp/warehouse/` directory if it doesn't exist
2. **Catalog Configuration** - Sets up SQLite catalog with file-based storage
3. **Namespace Creation** - Creates the specified namespace if it doesn't exist
4. **Table Creation** - For each specified Parquet file:
    - Reads the Parquet file (from S3 or local path)
    - Creates an Iceberg table with matching schema
    - Appends all data from the Parquet file to the Iceberg table
    - Skips files if tables already exist

## API Reference

### `build(catalog, parquet_file, namespace, table_name)`

!!! function "Main Build Function"
    Creates an Iceberg table from a single Parquet file (local or S3).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalog` | `Catalog` | PyIceberg catalog instance |
| `parquet_file` | `str` | Path to Parquet file (local path or S3 URL) |
| `namespace` | `str` | Iceberg namespace to create the table in |
| `table_name` | `str` | Name for the new table |

**Behavior:**
- Creates namespace if it doesn't exist
- Reads Parquet file schema using PyArrow
- Creates Iceberg table with matching schema
- Appends all data from the Parquet file
- Skips table creation if table already exists

**Example Usage:**
```python
from pyiceberg.catalog import load_catalog
from icefabric_manage import build

catalog = load_catalog("hydrofabric", **catalog_settings)
build(
    catalog=catalog,
    parquet_file="s3://hydrofabric-data/icefabric/hydrofabric/divides.parquet",
    namespace="hydrofabric",
    table_name="divides"
)
```

## Build Scripts

### Hydrofabric Build Script

The `build_hydrofabric.py` script processes multiple hydrofabric layers:

```python
layers = [
    "divide-attributes",
    "divides",
    "flowpath-attributes-ml",
    "flowpath-attributes",
    "flowpaths",
    "hydrolocations",
    "lakes",
    "network",
    "nexus",
    "pois"
]
```

Each layer is read from `s3://hydrofabric-data/icefabric/hydrofabric/{layer}.parquet` and created as a table in the `hydrofabric` namespace.

### USGS Streamflow Observations Build Script

The `build_usgs_streamflow_observations.py` script processes USGS hourly streamflow data:
- Source: `s3://hydrofabric-data/icefabric/streamflow_observations/usgs_hourly.parquet`
- Namespace: `observations`
- Table: `usgs_hourly`

## Configuration

### Catalog Settings

Both build scripts use these default settings:

```python
warehouse_path = Path("/tmp/warehouse")
catalog_settings = {
    "type": "sql",                                              # SQLite-backed catalog
    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",  # Database location
    "warehouse": f"file://{warehouse_path}",                    # Data storage location
}
```

### Data Sources

!!! note "Supported File Locations"
    - **S3 URLs**: `s3://bucket-name/path/to/file.parquet`
    - **Local paths**: `/path/to/local/file.parquet`
    - **Relative paths**: `./data/file.parquet`

!!! danger "Common Issues"
    - **S3 Access** - Ensure AWS credentials are properly configured
    - **Nanosecond timestamps** - Convert to microseconds before building
    - **Invalid schemas** - Verify Parquet files can be read with `pyarrow.parquet.read_table()`

## Customization

### Adding New Datasets

Create a new build script following this pattern:

```python
from pathlib import Path
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from icefabric_manage import build

load_dotenv()

if __name__ == "__main__":
    warehouse_path = Path("/tmp/warehouse")
    warehouse_path.mkdir(exist_ok=True)
    catalog_settings = {
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    }

    namespace = "your_namespace"
    catalog = load_catalog(namespace, **catalog_settings)

    build(
        catalog=catalog,
        parquet_file="path/to/your/file.parquet",
        namespace=namespace,
        table_name="your_table_name",
    )
    print("Build successful")
```

### Changing Warehouse Location

Modify the `warehouse_path` in your build script:

```python
warehouse_path = Path("/your/custom/warehouse/path")
```

### Alternative Catalog Backend

Replace SQLite with other supported backends:

```python
catalog_settings = {
    "type": "sql",
    "uri": "postgresql://user:pass@host:port/db",
    "warehouse": "s3://your-bucket/warehouse/",
}
```

## Best Practices

!!! tip "Recommendations"
    1. **Table Naming** - Use descriptive table names that reflect the data content
    2. **Data Preparation** - Ensure timestamps are properly formatted before building

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
