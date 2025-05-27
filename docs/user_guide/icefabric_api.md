# Icefabric API Guide

## Overview

The Icefabric API is a FastAPI-based service that provides access to EDFS data stored in Apache Iceberg format. The API offers multiple data export formats and comprehensive metadata endpoints for hydrology and water resources applications.

## Architecture

The API consists of several key components:

1. **Main Application** (`main.py`) - FastAPI application with health checks and router configuration
2. **Data Routers** - Handles all data endpoints. Currently only working with streamflow observations
3. **Apache Iceberg Backend** - SQLite-backed catalog stored in `/tmp/warehouse` built using `icefabric_manage`

## How It Works

!!! info "API Overview"
    The API provides RESTful endpoints to query, filter, and export streamflow observations in multiple formats (CSV, Parquet) with comprehensive metadata support.

### Data Flow

1. **Request Processing** - Validates data source and identifier parameters
2. **Data Filtering** - Applies optional date range filters to Iceberg tables
3. **Format Conversion** - Exports data in requested format (CSV/Parquet)
4. **Response Generation** - Returns data with appropriate headers and metadata

### Supported Data Sources

Currently supports:
- **USGS** - United States Geological Survey hourly streamflow data

!!! note "Data Storage"
    All data is stored as Apache Iceberg tables in a SQLite-backed catalog locally built at `/tmp/warehouse/pyiceberg_catalog.db`

## Usage Examples

### Python Client

```python
import requests
import pandas as pd
from io import StringIO, BytesIO

# Base URL
base_url = "http://localhost:8000/v1/observations"

# Get station information
response = requests.get(f"{base_url}/usgs/info", params={"identifier": "01031500"})
info = response.json()
print(f"Station has {info['total_records']} records")

# Download CSV data
csv_response = requests.get(
    f"{base_url}/usgs/csv",
    params={
        "identifier": "01031500",
        "start_date": "2023-01-01T00:00:00",
        "end_date": "2023-01-31T23:59:59"
    }
)
df_csv = pd.read_csv(StringIO(csv_response.text))

# Download Parquet data (more efficient for large datasets)
parquet_response = requests.get(
    f"{base_url}/usgs/parquet",
    params={
        "identifier": "01031500",
        "start_date": "2023-01-01T00:00:00"
    }
)
df_parquet = pd.read_parquet(BytesIO(parquet_response.content))
```

## Performance Considerations

### Data Format Recommendations

| Dataset Size | Recommended Format | Reason |
|-------------|-------------------|---------|
| < 50,000 records | CSV | Simple, widely supported |
| > 50,000 records | Parquet | Better compression, faster processing |
| > 200,000 records | Parquet + date filters | Reduced data transfer |

## Configuration

### Catalog Settings

The default catalog is pointing to a local instance. To build this catalog, one must use the build scripts in `icefabric_manage`

```python
catalog_settings = {
    "type": "sql",
    "uri": "sqlite:////tmp/warehouse/pyiceberg_catalog.db",
    "warehouse": "file:///tmp/warehouse",
}
```

## Development

### Running the API

```bash
# Install dependencies
uv sync

# Start development server
python src/icefabric_api/main.py
```

### Adding New Data Sources

To add a new data source, update the configuration in your router:

Below is an example for the observations router

```python
class DataSource(str, Enum):
    USGS = "usgs"
    NEW_SOURCE = "new_source"  # Add new source

# Add configuration
DATA_SOURCE_CONFIG = {
    DataSource.NEW_SOURCE: {
        "namespace": "observations",
        "table": "new_source_table",
        "time_column": "timestamp",
        "units": "m³/s",
        "description": "New data source description",
    },
}
```

## API Documentation

### Interactive Documentation

The API provides interactive documentation at:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

### OpenAPI Schema

Access the OpenAPI schema at: `http://localhost:8000/openapi.json`


### Verification

Test your API setup:

```bash

# List available sources
curl http://localhost:8000/v1/observations/sources

# Test with a known identifier
curl "http://localhost:8000/v1/observations/usgs/available?limit=1"

# Get a CSV reponse
curl "http://localhost:8000/v1/observations/usgs/csv?identifier=01010000&include_headers=true"

# Fitler based on timestamps
curl "http://localhost:8000/v1/observations/usgs/csv?identifier=01010000&start_date=2021-12-31T14%3A00%3A00&end_date=2022-01-01T14%3A00%3A00&include_headers=true"

# Get a Parquet Response
curl "http://localhost:8000/v1/observations/usgs/parquet\?identifier\=01010000\&start_date\=2021-12-31T14%3A00%3A00\&end_date\=2022-01-01T14%3A00%3A00" -o "output.parquet"
```
