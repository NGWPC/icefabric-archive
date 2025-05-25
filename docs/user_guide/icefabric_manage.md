# Table Building Guide

## Overview

IceFabric Manage is a Python package that simplifies building Apache Iceberg catalogs from Parquet files

## Architecture

The system consists of two main components:

1. **Core API** (`src/icefabric_manage/_api.py`) - Contains the table building logic
2. **Build Scripts** (`scripts/`) - Orchestrates the catalog creation process

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

## API Reference

### `build(catalog, file_dir, namespace)`

!!! function "Main Build Function"
    Orchestrates the catalog building process by scanning a directory and creating Iceberg tables from Parquet files.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalog` | `Catalog` | PyIceberg catalog instance |
| `file_dir` | `Path` | Directory containing Parquet files to process |
| `namespace` | `str` | Iceberg namespace to create tables in |

**Behavior:**
- Creates namespace if it doesn't exist
- Scans directory for `.parquet` files
- Creates tables using filename as table name
- Skips existing tables to avoid conflicts

### `_add_parquet_to_catalog(catalog, file_path, namespace, table_name)`

!!! function "Internal Table Creator"
    Creates an individual Iceberg table from a Parquet file. This function is called internally by `build()`.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalog` | `Catalog` | PyIceberg catalog instance |
| `file_path` | `Path` | Path to the Parquet file |
| `namespace` | `str` | Target namespace |
| `table_name` | `str` | Name for the new table |

**Returns:** `Table` - Iceberg Table object

**Raises:** `FileNotFoundError` - If the specified Parquet file doesn't exist

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

### Data Requirements

!!! note "Parquet File Constraints"
    - Must have valid Arrow schema
    - Timestamp columns must be microsecond precision or coarser  
    - Files should be properly formatted and readable by PyArrow

!!! danger "Common Issues"
    - **Nanosecond timestamps** - Convert to microseconds before building
    - **Missing files** - Ensure all Parquet files exist in the data directory
    - **Invalid schemas** - Verify Parquet files can be read with `pyarrow.parquet.read_table()`

## Customization

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

### Alternative Catalog Backend

Replace SQLite with other supported backends (PostgreSQL, etc.):

```python
catalog_settings = {
    "type": "sql",
    "uri": "postgresql://user:pass@host:port/db",
    "warehouse": "s3://your-bucket/warehouse/",
}
```

## Best Practices

!!! tip "Recommendations"
    1. **File Naming** - Use descriptive Parquet filenames as they become table names
    2. **Data Preparation** - Ensure timestamps are properly formatted before building  
    3. **Incremental Builds** - The system skips existing tables, making it safe to re-run
    4. **Testing** - Test with a small subset of files first
    5. **Backup** - Keep original Parquet files as the build process doesn't modify them

## Troubleshooting

### Common Errors

!!! failure "Timestamp Precision Error"
    ```text
    TypeError: Iceberg does not yet support 'ns' timestamp precision
    ```
    **Solution:** Convert timestamps to microsecond precision in your Parquet files

!!! failure "File Not Found"
    ```text
    FileNotFoundError: Cannot find parquet data dir
    ```
    **Solution:** Verify the `data_dir` path exists and contains `.parquet` files

!!! info "Schema Conflicts"
    ```text
    Table already exists
    ```
    **Solution:** This is expected behavior - existing tables are skipped safely

### Verification

After building, verify your catalog:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("hydrofabric", **catalog_settings)
print(catalog.list_tables("hydrofabric"))  # List all tables
table = catalog.load_table("hydrofabric.streamflow")  # Load specific table
print(table.scan().to_pandas())  # Query data
```

## Next Steps

Once tables are built, you can:
- Query data using PyIceberg's scan API
- Build FastAPI endpoints to serve data
- Connect to analytics tools that support Iceberg
- Perform time-series analysis on the structured data

The Iceberg format provides ACID transactions, schema evolution, and efficient querying capabilities for your hydrofabric datasets.
