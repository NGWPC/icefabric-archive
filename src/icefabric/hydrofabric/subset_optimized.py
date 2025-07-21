"""Functional hydrofabric subset implementation using pre-computed upstream lookup table with Polars"""

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import polars as pl
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import In

from icefabric.helpers.geopackage import to_geopandas
from icefabric.schemas.hydrofabric import HydrofabricDomains, IdType


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


def find_origin_flowpath(catalog: Catalog, domain: HydrofabricDomains, identifier: str, id_type: str) -> str:
    """
    Find the origin flowpath for a given identifier using Polars

    Parameters
    ----------
    catalog : Catalog
        PyIceberg catalog
    identifier : str
        The identifier to search for
    id_type : str
        Type of identifier

    Returns
    -------
    str
        The origin flowpath ID
    """
    # Load network table efficiently
    network_table = catalog.load_table(f"{domain.value}.network").to_polars()

    # Find matching network entries
    if id_type == "hl_uri":
        matches = network_table.filter(pl.col("hl_uri") == identifier).select("id").collect()
    else:
        raise ValueError(f"ID type {id_type} not yet implemented")

    if matches.height == 0:
        raise ValueError(f"No network entry found for {identifier}")

    network_ids = matches.get_column("id").to_list()

    if len(network_ids) == 1:
        return network_ids[0]
    else:
        # TODO: provide a solution for when multiple IDs correspond to the same gauge
        return network_ids[0]


def subset_layers(
    catalog: Catalog, domain: HydrofabricDomains, layers: list[str], upstream_ids: set[str]
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
    id_filter = In("id", upstream_ids_list)

    network = catalog.load_table(f"{domain.value}.network")
    filtered_network = network.scan(row_filter=id_filter).to_pandas()
    filtered_network["poi_id"] = filtered_network["poi_id"].apply(
        lambda x: str(int(x)) if pd.notna(x) else None
    )

    valid_divide_ids = filtered_network["divide_id"].dropna().drop_duplicates().values.tolist()
    assert valid_divide_ids, "No valid divide_ids found"

    flowpaths = catalog.load_table(f"{domain.value}.flowpaths")
    filtered_flowpaths = flowpaths.scan(row_filter=In("divide_id", valid_divide_ids)).to_pandas()
    assert len(filtered_flowpaths) > 0, "No flowpaths found"
    valid_toids = filtered_flowpaths["toid"].dropna().values.tolist()
    assert valid_toids, "No flowpaths found"
    filtered_flowpaths = to_geopandas(filtered_flowpaths)

    nexus = catalog.load_table(f"{domain.value}.nexus")
    filtered_nexus_points = nexus.scan(row_filter=In("id", valid_toids)).to_pandas()
    filtered_nexus_points = to_geopandas(filtered_nexus_points)
    filtered_nexus_points["poi_id"] = filtered_nexus_points["poi_id"].apply(
        lambda x: str(int(x)) if pd.notna(x) else None
    )

    valid_divide_ids_for_divides = filtered_flowpaths["divide_id"].dropna().values.tolist()
    assert valid_divide_ids_for_divides, "No divide IDs found"

    divides = catalog.load_table(f"{domain.value}.divides")
    filtered_divides = divides.scan(row_filter=In("divide_id", valid_divide_ids_for_divides)).to_pandas()
    filtered_divides = to_geopandas(filtered_divides)

    output_layers = {
        "flowpaths": filtered_flowpaths,
        "nexus": filtered_nexus_points,
        "divides": filtered_divides,
        "network": filtered_network,
    }

    if "lakes" in layers:
        lakes = catalog.load_table(f"{domain.value}.lakes")
        filtered_lakes = lakes.scan(row_filter=In("divide_id", valid_divide_ids_for_divides)).to_pandas()
        filtered_lakes = to_geopandas(filtered_lakes)
        output_layers["lakes"] = filtered_lakes

    if "divide-attributes" in layers:
        divides_attr = catalog.load_table(f"{domain.value}.divide-attributes")
        filtered_divide_attr = divides_attr.scan(
            row_filter=In("divide_id", valid_divide_ids_for_divides)
        ).to_pandas()
        output_layers["divide-attributes"] = filtered_divide_attr

    if "flowpath-attributes" in layers:
        flowpath_attr = catalog.load_table(f"{domain.value}.flowpath-attributes")
        valid_flowpath_ids = filtered_flowpaths["id"].dropna().values.tolist()
        filtered_flowpath_attr = flowpath_attr.scan(row_filter=In("id", valid_flowpath_ids)).to_pandas()
        output_layers["flowpath-attributes"] = filtered_flowpath_attr

    if "flowpath-attributes-ml" in layers:
        flowpath_attr_ml = catalog.load_table(f"{domain.value}.flowpath-attributes-ml")
        valid_flowpath_ids = filtered_flowpaths["id"].dropna().values.tolist()
        filtered_flowpath_attr_ml = flowpath_attr_ml.scan(row_filter=In("id", valid_flowpath_ids)).to_pandas()
        output_layers["flowpath-attributes-ml"] = filtered_flowpath_attr_ml

    if "pois" in layers:
        pois = catalog.load_table(f"{domain.value}.pois")
        poi_values = filtered_flowpaths["poi_id"].dropna().values
        filtered_poi_list = list({int(x) for x in poi_values if pd.notna(x)})
        if filtered_poi_list:
            filtered_pois = pois.scan(row_filter=In("poi_id", filtered_poi_list)).to_pandas()
            output_layers["pois"] = filtered_pois

    if "hydrolocations" in layers:
        hydrolocations = catalog.load_table(f"{domain.value}.hydrolocations")
        poi_values = filtered_flowpaths["poi_id"].dropna().values
        filtered_poi_list = list({int(x) for x in poi_values if pd.notna(x)})
        if filtered_poi_list:
            filtered_hydrolocations = hydrolocations.scan(
                row_filter=In("poi_id", filtered_poi_list)
            ).to_pandas()
            output_layers["hydrolocations"] = filtered_hydrolocations

    return output_layers


def subset_hydrofabric(
    catalog: Catalog,
    identifier: str,
    id_type: str,
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

    origin_id = find_origin_flowpath(catalog, domain, identifier, id_type)
    print(f"Found origin flowpath: {origin_id}")

    upstream_ids = get_upstream_segments(origin_id, upstream_dict)
    print(f"Found {len(upstream_ids)} upstream segments")

    output_layers = subset_layers(catalog=catalog, domain=domain, layers=layers, upstream_ids=upstream_ids)

    return output_layers


def subset_v2(
    catalog: Catalog,
    identifier: str,
    id_type: str,
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
    id_type : str
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


if __name__ == "__main__":
    catalog = load_catalog("glue")
    subset_v2(
        catalog=catalog,
        identifier="gages-01010000",
        id_type=IdType.HL_URI,
        layers=["divides", "flowpaths", "network", "nexus"],
        output_file=Path.cwd() / "subset_v2.gpkg",
        domain=HydrofabricDomains.CONUS,
    )
