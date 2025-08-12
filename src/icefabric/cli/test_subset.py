"""Contains all click CLI code for the hydrofabric"""

from pathlib import Path

import geopandas as gpd

from icefabric.builds.graph_connectivity import load_upstream_json
from icefabric.cli import get_catalog
from icefabric.helpers import load_creds
from icefabric.hydrofabric.subset import subset_hydrofabric
from icefabric.schemas.hydrofabric import HydrofabricDomains, IdType

load_creds(dir=Path(__file__).parents[3])


if __name__ == "__main__":
    id_type = "hl_uri"
    identifier = "gages-01010000"
    domain = HydrofabricDomains.CONUS.value
    layers = None
    id_type_enum = IdType(id_type)
    _catalog = get_catalog("glue")

    connectivity_graphs = load_upstream_json(
        catalog=_catalog,
        namespaces=[domain],
        output_path=Path(__file__).parents[3] / "data",
    )

    layers_list = list(layers) if layers else ["divides", "flowpaths", "network", "nexus"]

    output_layers = subset_hydrofabric(
        catalog=_catalog,
        identifier=identifier,
        id_type=id_type_enum,
        layers=layers_list,
        namespace=domain,
        graph=connectivity_graphs[domain],
    )

    output_file = Path.cwd() / "subset.gpkg"

    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file:
        for table_name, _layer in output_layers.items():
            if len(_layer) > 0:  # Only save non-empty layers
                gpd.GeoDataFrame(_layer).to_file(output_file, layer=table_name, driver="GPKG")
            else:
                print(f"Warning: {table_name} layer is empty")
