# icefabric

<img src="docs/img/icefabric.png" alt="icefabric" width="50%"/>

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)


An [Apache Iceberg](https://py.iceberg.apache.org/) implementation of the Hydrofabric to disseminate continental hydrologic data

### Getting Started
This repo is managed through [UV](https://docs.astral.sh/uv/getting-started/installation/) and can be installed through:
```sh
uv venv
source .venv/bin/activate
uv sync
```

### Running the API locally
To run the API locally, ensure your `.env` file in your project root has the right credentials, then run
```sh
uv pip install -e src/icefabric_api
cd src/icefabric_api
python -m app.main
```
This should spin up the API services

### Building the API through Docker
To run the API locally with Docker, ensure your `.env` file in your project root has the right credentials, then run
```sh
docker compose -f docker/compose.yaml build --no-cache
docker compose -f docker/compose.yaml up
```
This should spin up the API services
```


### Development
To ensure that icefabric follows the specified structure, be sure to install the local dev dependencies and run `pre-commit install`

### Documentation
To build the user guide documentation for Icefabric locally, run the following commands:
```sh
uv pip install ".[docs]"
mkdocs serve -a localhost:8080
```
Docs will be spun up at localhost:8080/
