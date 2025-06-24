# Topobathy Data Viewer

## If tiles are pre-created and stored on s3:
1. Move to icechunk_data-viewer folder:
    `cd examples/icechunk_data_viewer`
2. Export AWS data account credentials to your environment and run in CLI:
    `aws s3 sync s3://hydrofabric-data/surface/nws-topobathy/tiles ./martin/tiles`

3. Start martin tile server:

    `cd martin`

    `docker compose -f compose.martin.yaml up`

4. Tile server is now running. Confirm by checking `localhost:3000/catalog`. You should see a list of tile sources. Debug logs should be populating in console.
5. Return  `icechunk_data_viewer` root

    `cd ..`
6. Start jupyter lab in activated icefabric virtual environment. This will start the jupyter server at `localhost:8888/lab`

    `jupyter lab`

7. Open `viewer.ipynb` in Jupyter Lab
8.  Execute cells in `viewer.ipynb`. The map should show the tiles served from Martin.


## Full Pipeline - from creating tiles to viewing
1. __In icefabric repo__: Export topobathy from icechunk to TIFF using `icefabric_tools.icechunk.IcechunkS3Repo`
2. Initialize the s3 repo and convert to tiff.

    ```
    from icefabric_tools.icechunk import IcechunkS3Repo, NGWPCLocations

    repo = IcechunkS3Repo(location=NGWPCLocations.TOPO_HA_30M_IC.path)
    ds = repo.retrieve_dataset()
    repo.retrieve_and_convert_to_tiff(dest=<YOUR PATH HERE>, var_name='elevation')
    ```

3. Clone hydrofabric-ui-tools
4. __In hydrofabric-ui-tools repo__: Copy saved TIFFs to `data` folder in `hydrofabric-ui-tools`
5. Create or modify `config` files to match TIFF
6. Run `build_topobathy_tiles.py` using docker or local environment as described in `README.md`
7. Tiles will be uploaded to s3 if specified in config.
8. __Return to icefabric repo__
9. Sync from s3 or paste `.pmtiles` files into

    `icefabric/examples/icechunk_data_viewer/martin/tiles`

    AWS option with data account credentials in env vars.
    ```
    cd examples/icechunk_data_viewer
    aws s3 sync s3://hydrofabric-data/surface/nws-topobathy/tiles ./martin/tiles
    ```

10. Open `martin_config.yaml`

    `icefabric/examples/icechunk_data_viewer/martin/martin_config.yaml`

11. Match tile names in `tiles` folders to source name. Source name will be the URI for tile serving.

12. Start martin tile server. This must be done in the `martin` working directory to copy the files correctly.
    ```
    cd examples/icechunk_data/viewer/martin
    docker compose -f compose.martin.yaml up
    ```

13. Tile server is now running. Confirm by checking `localhost:3000/catalog`. You should see a list of tile sources.
14. Return root to `icechunk_data_viewer`

    `cd ..`
15. Start jupyter lab in activated icefabric virtual environment. This will start the jupyter server at `localhost:8888/lab`

    `jupyter lab`

16. Open `viewer.ipynb` in Jupyter Lab
17. Execute cells in `viewer.ipynb`. The map should show the tiles served from Martin.
