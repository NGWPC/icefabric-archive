# Topobathy Data Viewer

## If tiles are already made,
1. `cd` into `icechunk_data_viewer`
2. Export AWS data credentials to your environment and run in CLI: `aws s3 sync s3://hydrofabric_data/surface/nws-topobathy/tiles ./martin/tiles`
3. Start martin tile server: `docker compose -f compose.martin.yaml up`
4. Tile server is now running. Confirm by checking `localhost:3000/catalog`. You should see a list of tile sources.
5. At `icechunk_data_viewer` root, start jupyter lab with command: `jupyter lab`
6. Open `viewer.ipynb` in Jupyter Lab
7.  Execute cells in `viewer.ipynb`. The map should show the tiles served from Martin.


## Full Pipeline - from creating tiles to viewing
1. __In icefabric repo__: Export topobathy from icechunk to TIFF using `icefabric_tools.icechunk.IcechunkS3Repo`.
2. Once S3 repo is initialized, `repo.retrieve_and_convert_to_tiff(dest=[path], var_name='elevation')`
3. Clone hydrofabric-ui-tools
4. __In hydrofabric-ui-tools repo__: Copy saved TIFFs to `data` folder in `hydrofabric-ui-tools`
5. Create or modify `config` files to match TIFF
6. Run `build_topobathy_tiles.py` using docker or local environment as described in `README.md`
7. Tiles will be uploaded to s3 if specified in config.
8. __Return to icefabric repo__
9. Download from s3 or paste `.pmtiles` files into `icefabric/examples/icechunk_data_viewer/martin/tiles`
10. Open `martin_config.yaml` (`icefabric/examples/icechunk_data_viewer/martin/martin_config.yaml`)
11. Match tile names in `tiles` folders to source name. Source name will be the URI for tile serving.
12. Once tile names are set in `martin_config.yaml`, `cd` into `martin`
13. Start martin tile server: `docker compose -f compose.martin.yaml up`
14. Tile server is now running. Confirm by checking `localhost:3000/catalog`. You should see a list of tile sources.
15. `cd ..` so that your root is `icechunk_data_viewer`
16. With `icefabric` virtual environment, run in CLI `jupyter lab`. This will start the jupyter server at `localhost:8888/lab`
17. Open `viewer.ipynb` in Jupyter Lab
18. Execute cells in `viewer.ipynb`. The map should show the tiles served from Martin.