"""Functional hydrofabric subset implementation using pre-computed upstream lookup table with Polars"""

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo, In

from icefabric.helpers.geopackage import to_geopandas
from icefabric.hydrofabric.origin import find_origin
from icefabric.schemas.hydrofabric import UPSTREAM_VPUS, HydrofabricDomains, IdType


def get_upstream_segments(origin: str, upstream_dict: dict[str, list[str]]) -> set[str]:
    """Subsets the hydrofabric to find all upstream watershed boundaries upstream of the origin fp

    Parameters
    ----------
    origin: str
        The starting point where we're tracing upstream
    upstream_dict: dict[str, list[str]]
        a dictionary which preprocesses all toid -> id relationships

    Returns
    -------
    set[str]
        The watershed boundary connections that make up the subset
    """
    upstream_ids = set()
    stack = [origin]

    while stack:
        current_id = stack.pop()

        if current_id in upstream_ids:
            continue

        upstream_ids.add(current_id)

        # Add all upstream segments to the stack
        if current_id in upstream_dict:
            for upstream_id in upstream_dict[current_id]:
                if upstream_id not in upstream_ids:
                    stack.append(upstream_id)

    return upstream_ids


def subset_layers(
    catalog: Catalog,
    domain: HydrofabricDomains,
    layers: list[str],
    upstream_ids: set[str],
    vpu_id: str,
) -> dict[str, pd.DataFrame | gpd.GeoDataFrame]:
    """
    Efficiently subset a layer using Polars and the upstream IDs

    Parameters
    ----------
    catalog : Catalog
        PyIceberg catalog
    layer_name : str
        Name of the layer to subset
    upstream_ids : Set[str]
        Set of upstream flowpath IDs to include

    Returns
    -------
    pl.LazyFrame
        Lazy frame with only the upstream segments
    """
    # Ensuring there are always divides, flowpaths, network, and nexus layers
    if layers is None:
        layers = []
    layers.extend(["divides", "flowpaths", "network", "nexus"])
    layers = list(set(layers))

    upstream_ids_list = list(upstream_ids)

    # Create VPU filter
    if vpu_id in UPSTREAM_VPUS:
        # Use upstream VPUs mapping if available
        vpu_filter = In("vpuid", UPSTREAM_VPUS[vpu_id])
    else:
        # Use single VPU filter
        vpu_filter = EqualTo("vpuid", vpu_id)

    print("Subsetting network layer")
    network = catalog.load_table(f"{domain.value}.network").scan(row_filter=vpu_filter).to_polars()
    filtered_network = network.filter(
        pl.col("id").is_in(upstream_ids_list) | pl.col("toid").is_in(upstream_ids_list)
    ).with_columns(
        pl.col("poi_id").map_elements(lambda x: str(int(x)) if x is not None else None, return_dtype=pl.Utf8)
    )

    print("Subsetting flowpaths layer")
    flowpaths = catalog.load_table(f"{domain.value}.flowpaths").scan(row_filter=vpu_filter).to_polars()
    filtered_flowpaths = flowpaths.filter(pl.col("id").is_in(upstream_ids_list))
    assert filtered_flowpaths.height > 0, "No flowpaths found"
    filtered_flowpaths_geo = to_geopandas(filtered_flowpaths.to_pandas())

    print("Subsetting nexus layer")
    valid_toids = filtered_flowpaths.filter(pl.col("toid").is_not_null()).get_column("toid").to_list()
    assert valid_toids, "No nexus points found"
    nexus = catalog.load_table(f"{domain.value}.nexus").scan(row_filter=vpu_filter).to_polars()
    filtered_nexus_points = nexus.filter(pl.col("id").is_in(valid_toids)).with_columns(
        pl.col("poi_id").map_elements(lambda x: str(int(x)) if x is not None else None, return_dtype=pl.Utf8)
    )
    filtered_nexus_points_geo = to_geopandas(filtered_nexus_points.to_pandas())

    print("Subsetting divides layer")
    valid_divide_ids = (
        filtered_network.filter(pl.col("divide_id").is_not_null()).get_column("divide_id").unique().to_list()
    )
    assert valid_divide_ids, "No valid divide_ids found"
    divides = catalog.load_table(f"{domain.value}.divides").scan(row_filter=vpu_filter).to_polars()
    filtered_divides = divides.filter(pl.col("divide_id").is_in(valid_divide_ids))
    filtered_divides_geo = to_geopandas(filtered_divides.to_pandas())

    output_layers = {
        "flowpaths": filtered_flowpaths_geo,
        "nexus": filtered_nexus_points_geo,
        "divides": filtered_divides_geo,
        "network": filtered_network.to_pandas(),  # Convert to pandas for final output
    }

    if "lakes" in layers:
        print("Subsetting lakes layer")
        lakes = catalog.load_table(f"{domain.value}.lakes").scan(row_filter=vpu_filter).to_polars()
        filtered_lakes = lakes.filter(pl.col("divide_id").is_in(valid_divide_ids))
        filtered_lakes_geo = to_geopandas(filtered_lakes.to_pandas())
        output_layers["lakes"] = filtered_lakes_geo

    if "divide-attributes" in layers:
        print("Subsetting divide-attributes layer")
        divides_attr = (
            catalog.load_table(f"{domain.value}.divide-attributes").scan(row_filter=vpu_filter).to_polars()
        )
        filtered_divide_attr = divides_attr.filter(pl.col("divide_id").is_in(valid_divide_ids))
        output_layers["divide-attributes"] = filtered_divide_attr.to_pandas()

    if "flowpath-attributes" in layers:
        print("Subsetting flowpath-attributes layer")
        flowpath_attr = (
            catalog.load_table(f"{domain.value}.flowpath-attributes").scan(row_filter=vpu_filter).to_polars()
        )
        filtered_flowpath_attr = flowpath_attr.filter(pl.col("id").is_in(upstream_ids_list))
        output_layers["flowpath-attributes"] = filtered_flowpath_attr.to_pandas()

    if "flowpath-attributes-ml" in layers:
        print("Subsetting flowpath-attributes-ml layer")
        flowpath_attr_ml = (
            catalog.load_table(f"{domain.value}.flowpath-attributes-ml")
            .scan(row_filter=vpu_filter)
            .to_polars()
        )
        filtered_flowpath_attr_ml = flowpath_attr_ml.filter(pl.col("id").is_in(upstream_ids_list))
        output_layers["flowpath-attributes-ml"] = filtered_flowpath_attr_ml.to_pandas()

    if "pois" in layers:
        print("Subsetting pois layer")
        pois = catalog.load_table(f"{domain.value}.pois").scan(row_filter=vpu_filter).to_polars()
        filtered_pois = pois.filter(pl.col("id").is_in(upstream_ids_list))
        output_layers["pois"] = filtered_pois.to_pandas()

    if "hydrolocations" in layers:
        print("Subsetting hydrolocations layer")
        hydrolocations = (
            catalog.load_table(f"{domain.value}.hydrolocations").scan(row_filter=vpu_filter).to_polars()
        )
        filtered_hydrolocations = hydrolocations.filter(pl.col("id").is_in(upstream_ids_list))
        output_layers["hydrolocations"] = filtered_hydrolocations.to_pandas()

    return output_layers


def subset_hydrofabric(
    catalog: Catalog,
    identifier: str,
    id_type: IdType,
    layers: list[str],
    domain: HydrofabricDomains,
    upstream_dict: dict[str, list[str]],
) -> dict[str, pd.DataFrame | gpd.GeoDataFrame]:
    """
    Main subset function using pre-computed upstream lookup

    Parameters
    ----------
    catalog : Catalog
        PyIceberg catalog
    identifier : str
        The identifier to subset around
    id_type : str
        Type of identifier
    layers : List[str]
        List of layers to subset
    domain : str
        Domain name
    upstream_dict : Dict[str, Set[str]]
        Pre-computed upstream lookup dictionary

    Returns
    -------
    Dict[str, pl.LazyFrame]
        Dictionary of layer names to their subsetted lazy frames
    """
    print(f"Starting subset for {identifier}")

    network_table = catalog.load_table(f"{domain.value}.network").to_polars()
    origin_row = find_origin(network_table, identifier, id_type)
    origin_id = origin_row.select(pl.col("id")).item()
    to_id = origin_row.select(pl.col("toid")).item()
    vpu_id = origin_row.select(pl.col("vpuid")).item()
    print(f"Found origin flowpath: {origin_id}")

    upstream_ids = get_upstream_segments(origin_id, upstream_dict)
    print(f"Found {len(upstream_ids)} upstream segments")
    upstream_ids.add(to_id)  # Adding the nexus point to ensure it's captured in the network table

    output_layers = subset_layers(
        catalog=catalog, domain=domain, layers=layers, upstream_ids=upstream_ids, vpu_id=vpu_id
    )

    return output_layers


def subset(
    catalog: Catalog,
    identifier: str,
    id_type: IdType,
    layers: list[str],
    output_file: Path,
    domain: HydrofabricDomains,
) -> None | dict[str, pd.DataFrame | gpd.GeoDataFrame]:
    """
    Optimized subset function using pre-computed upstream lookup

    Parameters
    ----------
    catalog : Catalog
        PyIceberg catalog
    identifier : str
        The identifier to subset around
    id_type : IdType
        Type of identifier
    layers : List[str]
        List of layers to subset
    output_file : Path
        Output file path
    domain : HydrofabricDomains
        Domain name
    lookup_storage : str
        Storage type for upstream lookup table
    lookup_path : Path, optional
        Path to upstream lookup table
    """
    # Create or load upstream lookup table
    upstream_connections_path = (
        Path(__file__).parents[3] / f"data/hydrofabric/{domain.value}_upstream_connections.json"
    )
    assert upstream_connections_path.exists(), (
        f"Upstream Connections missing for {domain.value}. Please run `icefabric build-upstream-connections` to generate this file"
    )

    with open(upstream_connections_path) as f:
        data = json.load(f)
        print(
            f"Loading upstream connections connected generated on: {data['_metadata']['generated_at']} from snapshot id: {data['_metadata']['iceberg']['snapshot_id']}"
        )
        upstream_dict = data["upstream_connections"]

    output_layers = subset_hydrofabric(
        catalog=catalog,
        identifier=identifier,
        id_type=id_type,
        layers=layers,
        domain=domain,
        upstream_dict=upstream_dict,
    )

    # Write results
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file:
        for table_name, _layer in output_layers.items():
            if len(_layer) > 0:  # Only save non-empty layers
                gpd.GeoDataFrame(_layer).to_file(output_file, layer=table_name, driver="GPKG")
            else:
                print(f"Warning: {table_name} layer is empty")
        return None
    else:
        return output_layers


# if __name__ == "__main__":
#     catalog = load_catalog("sql")
#     subset_v2(
#         catalog=catalog,
#         identifier="gages-01010000",
#         id_type=IdType.HL_URI,
#         layers=["divides", "flowpaths", "network", "nexus"],
#         output_file=Path.cwd() / "subset_v2.gpkg",
#         domain=HydrofabricDomains.CONUS,
#     )
