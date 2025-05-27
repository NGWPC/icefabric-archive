# Icefabric Manage

A repo which contains scripts for building the icefabric from scratch

### How to build locally
- The `icefabric_manage` package must be installed through `uv sync`
- There must be parquet files locally to build from. These files are to be downloaded from the NGWPC S3 bucket @ `s3://hydrofabric-data/icefabric/` and placed in `src/icefabric_manage/data/`

Once you have parquet files locally in the `data/` dir, you can convert them to an icefabric warehouse through a build script in `scripts/` or make your own!
