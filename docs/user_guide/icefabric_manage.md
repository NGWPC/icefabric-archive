# Table Building Guide

## Overview

IceFabric Manage is a Python package that simplifies building Apache Iceberg catalogs from Parquet files stored on S3 or locally.

## Architecture

The system consists of two main components:

1. **Core API** (`src/icefabric_manage/_api.py`) - Contains the table building logic
2. **Build Scripts** (`scripts/`) - Orchestrates the catalog creation process for specific datasets

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
│   └── _api.py                      # Core table building functions
├── scripts/
│   ├── build_hydrofabric.py         # Hydrofabric dataset build script
│   └── build_usgs_streamflow_observations.py  # USGS observations build script
/tmp/warehouse/                  # Iceberg catalog storage (auto-created)
└── pyiceberg_catalog.db
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
- Source: `s3://hydrofabric-data/icefabric/observations/usgs_hourly.parquet`
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
    3. **Incremental Builds** - The system skips existing tables, making it safe to re-run
    4. **Environment Variables** - Use `.env` files for configuration management
    5. **S3 Credentials** - Ensure proper AWS credentials for S3 access
    6. **Testing** - Test with individual files before processing large datasets

## Troubleshooting

### Common Errors

!!! failure "Timestamp Precision Error"
    ```text
    TypeError: Iceberg does not yet support 'ns' timestamp precision
    ```
    **Solution:** Convert timestamps to microsecond precision in your Parquet files

!!! failure "S3 Access Denied"
    ```text
    AccessDenied: Access Denied
    ```
    **Solution:** Verify AWS credentials and S3 bucket permissions

!!! failure "File Not Found"
    ```text
    FileNotFoundError: Cannot find parquet file
    ```
    **Solution:** Verify the S3 URL or local file path is correct

!!! info "Table Already Exists"
    ```text
    Table {table_name} already exists. Skipping build
    ```
    **Solution:** This is expected behavior - existing tables are skipped safely

### Verification

After building, verify your catalog:

```python
from pyiceberg.catalog import load_catalog

catalog_settings = {
    "type": "sql",
    "uri": "sqlite:////tmp/warehouse/pyiceberg_catalog.db",
    "warehouse": "file:///tmp/warehouse",
}

catalog = load_catalog("hydrofabric", **catalog_settings)
print(catalog.list_tables("hydrofabric"))  # List all tables
table = catalog.load_table("hydrofabric.divides")  # Load specific table
print(table.scan().to_pandas().head())  # Query sample data
```

## Next Steps

Once tables are built, you can:
- Query data using PyIceberg's scan API
- Build FastAPI endpoints to serve data
- Connect to analytics tools that support Iceberg
- Perform time-series analysis on the structured data
- Use the tables for geospatial analysis and visualization

The Iceberg format provides ACID transactions, schema evolution, and efficient querying capabilities for your hydrofabric and observational datasets.
