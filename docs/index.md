# Welcome to Icefabric

!!! warning "In Progress"
    These docs are a work in progress and will continously be updated

# Icefabric

An [Apache Iceberg](https://py.iceberg.apache.org/)/[Icechunk](https://icechunk.io/en/latest/) implementation of the Hydrofabric to disseminate continental hydrologic data

!!! note
    sTo run any of the functions in this repo your AWS test account credentials need to be in your `.env` file and your `.pyiceberg.yaml` settings need to up to date

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


### Development
To ensure that icefabric follows the specified structure, be sure to install the local dev dependencies and run `pre-commit install`
