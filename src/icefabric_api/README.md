# Icefabric API

FastAPI service providing access to hydrology and water resources data stored in Apache Iceberg format.

## Features

- **Streamflow Observations** - USGS hourly streamflow data in CSV/Parquet formats
- **Hydrofabric Subsets** - Upstream watershed data as downloadable geopackages
- **Interactive Documentation** - Built-in Swagger UI and ReDoc

## Quick Start

### Prerequisites

- [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
# Clone repository
git clone <repository-url>
cd icefabric

# Install package in development mode
uv pip install src/icefabric_api -e .
```

### Running the API

```bash
# Start development server
cd src/icefabric_api
python -m app.main
```

The API will be available at `http://localhost:8000`

### Docker Alternative

```bash
# Build and run with Docker
docker compose -f docker/compose.yaml build --no-cache
docker compose -f docker/compose.yaml up
```

## API Endpoints

### Streamflow Observations

- `GET /v1/streamflow_observations/sources` - List available data sources
- `GET /v1/streamflow_observations/usgs/available` - Get available station IDs
- `GET /v1/streamflow_observations/usgs/{identifier}/info` - Station metadata
- `GET /v1/streamflow_observations/usgs/csv` - Download CSV data
- `GET /v1/streamflow_observations/usgs/parquet` - Download Parquet data

### Hydrofabric

- `GET /v1/hydrofabric/{identifier}/gpkg` - Download watershed subset as geopackage

### Health Check

- `HEAD /health` - API health status

## Usage Examples

### Download Streamflow Data

```python
import requests
import pandas as pd

# Get station information
response = requests.get(
    "http://localhost:8000/v1/streamflow_observations/usgs/01031500/info"
)
station_info = response.json()

# Download CSV data
csv_response = requests.get(
    "http://localhost:8000/v1/streamflow_observations/usgs/csv",
    params={
        "identifier": "01031500",
        "start_date": "2023-01-01T00:00:00",
        "end_date": "2023-01-31T23:59:59"
    }
)
df = pd.read_csv(csv_response.text)
```

## Documentation

- **Interactive API Docs**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

## Initial Parameters

CSV files for catchment specific values are too big to be checked in.  They are stored here:  s3://ngwpc-dev/DanielCumpton/divide_csv.tgz
Download and untar in the icefabric/src/icefabric_api/data directory.
For now, you can run modules by listing them in the module list defined on line 108 in run_modules.py.  Text files showing all parameters as key/value
pairs will be written to a files per catchment.  A JSON is output to the terminal.  SFT, SMP, and LASAM csv files still need work to define the parameters properly.
See icefabric/src/icefabric_api/data/modules.csv for module name strings.
