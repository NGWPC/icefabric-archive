"""A file to hold all hydrofabric geospatial scripts"""

from enum import Enum

import geopandas as gpd
import pandas as pd
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, EqualTo, In

from icefabric_tools.iceberg.utils import find_origin, table_to_geopandas, to_geopandas


class IdType(str, Enum):
    """All queriable HF fields"""

    HL_URI = "hl_uri"
    HF_ID = "hf_id"
    ID = "id"
    POI_ID = "poi_id"


def get_sorted_network(network_df, outlets):
    """Python implementation similar to R's nhdplusTools::get_sorted()

    Parameters
    ----------
    network_df : pandas.DataFrame
        Network data with 'id' and 'toid' columns
    outlets : list or str
        Outlet IDs to start tracing from

    Returns
    -------
    pandas.DataFrame
        Sorted network topology with 'id' and 'toid' columns
    """
    if isinstance(outlets, str):
        outlets = [outlets]
    # Create a mapping of toid -> list of ids that flow into it
    downstream_map = {}
    all_segments = set()

    # Build the network mapping
    for _, row in network_df.iterrows():
        segment_id = row["id"]
        toid = row["toid"]
        all_segments.add(segment_id)
        if pd.notna(toid):
            if toid not in downstream_map:
                downstream_map[toid] = []
            downstream_map[toid].append(segment_id)

    visited = set()
    sorted_pairs = []
    upstream_segments = set()

    def trace_upstream_recursive(current_id):
        """Recursively trace upstream from current_id"""
        if current_id in visited:
            return
        visited.add(current_id)
        upstream_segments.add(current_id)

        # Find all segments that flow into current_id
        if current_id in downstream_map:
            for upstream_id in downstream_map[current_id]:
                if upstream_id not in visited:
                    sorted_pairs.append({"id": upstream_id, "toid": current_id})
                    trace_upstream_recursive(upstream_id)

    # Start tracing from each outlet
    for outlet in outlets:
        if outlet in all_segments or outlet in downstream_map:
            trace_upstream_recursive(outlet)

    # Additional pass: find any headwater segments that might have been missed
    # These are segments that don't appear as 'toid' for any other segment
    # but are connected to our upstream network
    connected_segments = set(upstream_segments)

    # Look for segments that flow into our connected network but weren't found
    for _, row in network_df.iterrows():
        segment_id = row["id"]
        toid = row["toid"]

        # If this segment flows into something we've already found,
        # but we haven't included this segment yet, add it
        if (
            pd.notna(toid)
            and toid in connected_segments
            and segment_id not in connected_segments
            and segment_id not in visited
        ):
            sorted_pairs.append({"id": segment_id, "toid": toid})
            connected_segments.add(segment_id)

    # Specific fix for nexus points: Include nexus points that are targets of our upstream segments
    # but weren't included in the original trace
    all_toids_in_pairs = {pair["toid"] for pair in sorted_pairs}
    for _, row in network_df.iterrows():
        segment_id = row["id"]
        toid = row["toid"]

        # If this segment flows to a nexus point that should be included
        if (
            segment_id in connected_segments
            and pd.notna(toid)
            and toid.startswith("nex-")
            and toid not in all_toids_in_pairs
            and toid not in connected_segments
        ):
            # Add the nexus point as a target
            sorted_pairs.append({"id": segment_id, "toid": toid})
            connected_segments.add(toid)
            print(f"Added missing nexus point: {toid}")

    return pd.DataFrame(sorted_pairs)


def get_all_upstream_ids(network, origin_data):
    """Get all upstream segment IDs using topology sorting approach

    Parameters
    ----------
    network : pyiceberg.table.Table
        The network table from iceberg catalog
    origin_data : pandas.DataFrame
        Single row representing the origin point

    Returns
    -------
    list
        All upstream segment IDs
    """
    # Filter network by VPU if available to reduce data size
    network_filters = []

    if "vpuid" in origin_data.columns and pd.notna(origin_data["vpuid"].iloc[0]):
        vpuid_filter = EqualTo("vpuid", origin_data["vpuid"].iloc[0])
        network_filters.append(vpuid_filter)

    # Apply filters to get network subset
    if network_filters:
        combined_filter = network_filters[0]
        for filter_expr in network_filters[1:]:
            combined_filter = And(combined_filter, filter_expr)
        network_data = network.scan(row_filter=combined_filter).to_pandas()
    else:
        # Get all network data - might be large, consider adding other filters
        network_data = network.scan().to_pandas()

    # Select only needed columns for topology sorting
    network_subset = network_data[["id", "toid"]].dropna(subset=["id"]).drop_duplicates()

    # Get the outlet (toid of the origin)
    outlets = origin_data["toid"].dropna().drop_duplicates().tolist()

    if not outlets:
        print("Warning: No outlets found for origin")
        return []

    print(f"Tracing upstream from outlets: {outlets}")
    # Use topology sorting to find all upstream segments
    topology = get_sorted_network(network_subset, outlets)

    assert len(topology) != 0, "No upstream topology found for ID"

    print(f"Found {len(topology)} topology connections")

    # Don't remove the last row initially - include more segments like R version
    topology = topology.drop_duplicates()

    # Get all unique IDs from topology (include both directions)
    all_ids = pd.concat([topology["id"], topology["toid"]]).dropna().unique().tolist()

    # Also include the origin ID itself
    origin_id = origin_data["id"].iloc[0]
    if origin_id not in all_ids:
        all_ids.append(origin_id)
    return all_ids


def subset(
    catalog: Catalog,
    identifier: str,
    id_type: IdType,
    layers: list[str] | None = None,
    output_file: str | None = None,
):
    """Returns a geopackage subset from the hydrofabric (Based on logic from HfsubsetR)

    Parameters
    ----------
    catalog : Catalog
        The iceberg catalog of the hydrofabric
    identifier : str
        The identifier to start tracing from
    id_type : IdType
        The type of identifier being used
    output_file : str
        The output file where we want to save the geopackage
    """
    if layers is None:
        layers = ["divides", "flowpaths", "network", "nexus"]
    network = catalog.load_table("hydrofabric.network")
    divides = catalog.load_table("hydrofabric.divides")
    flowpaths = catalog.load_table("hydrofabric.flowpaths")
    nexus = catalog.load_table("hydrofabric.nexus")

    # Find the origin point
    origin_row = find_origin(network_table=network, identifier=identifier, id_type=id_type.value)
    origin_filter = EqualTo("id", origin_row["id"].iloc[0])
    origin_data = network.scan(row_filter=origin_filter).to_pandas()

    # Get all upstream segment IDs using topology sorting approach (like R version)
    all_upstream_ids = get_all_upstream_ids(network, origin_data)

    if len(all_upstream_ids) == 0:
        print("No upstream segments found")
        return

    print(f"Found {len(all_upstream_ids)} upstream segments")

    # Get the full network data for upstream segments
    # Filter by VPU first if available to reduce data size
    if "vpuid" in origin_data.columns and pd.notna(origin_data["vpuid"].iloc[0]):
        vpuid_filter = EqualTo("vpuid", origin_data["vpuid"].iloc[0])
        network_base_filter = vpuid_filter
    else:
        network_base_filter = None

    # Combine with ID filter to get all catchment IDs
    id_filter = In("id", all_upstream_ids)
    if network_base_filter:
        combined_filter = And(network_base_filter, id_filter)
    else:
        combined_filter = id_filter

    all_upstream = network.scan(row_filter=combined_filter).to_pandas()

    filtered_network = network.scan(row_filter=In("id", all_upstream["id"].values)).to_pandas()

    # Get flowpaths for all upstream segments (filter out None values)
    valid_divide_ids = all_upstream["divide_id"].dropna().drop_duplicates().values.tolist()
    assert valid_divide_ids is not None, "No valid divide_ids found"
    filtered_flowpaths = flowpaths.scan(row_filter=In("divide_id", valid_divide_ids)).to_pandas()

    assert len(filtered_flowpaths) > 0, "No flowpaths found"
    valid_toids = filtered_flowpaths["toid"].dropna().values.tolist()  # Filter to remove None values
    assert valid_toids, "No flowpaths found"
    filtered_nexus_points = table_to_geopandas(table=nexus, row_filter=In("id", valid_toids))
    if len(filtered_flowpaths) > 0:
        filtered_flowpaths = to_geopandas(filtered_flowpaths)
    else:
        filtered_flowpaths = gpd.GeoDataFrame()

    valid_divide_ids_for_divides = filtered_flowpaths["divide_id"].dropna().values.tolist()
    assert valid_divide_ids_for_divides is not None, "No divide IDS found"
    filtered_divides = table_to_geopandas(
        table=divides, row_filter=In("divide_id", valid_divide_ids_for_divides)
    )

    output_layers = {
        "flowpaths": filtered_flowpaths,
        "nexus": filtered_nexus_points,
        "divides": filtered_divides,
        "network": filtered_network,
    }
    # Getting any optional fields:
    if "divide-attributes" in layers:
        divides_attr = catalog.load_table("hydrofabric.divide-attributes")
        filtered_divide_attr = divides_attr.scan(
            row_filter=In("divide_id", valid_divide_ids_for_divides)
        ).to_pandas()
        output_layers["divide-attributes"] = (filtered_divide_attr,)

    if "flowpath-attributes" in layers:
        flowpath_attr = catalog.load_table("hydrofabric.flowpath-attributes")
        valid_flowpath_ids = filtered_flowpaths["id"].dropna().values.tolist()
        filtered_flowpath_attr = flowpath_attr.scan(row_filter=In("id", valid_flowpath_ids)).to_pandas()
        output_layers["flowpath-attributes"] = (filtered_flowpath_attr,)

    if "flowpath-attributes-ml" in layers:
        flowpath_attr_ml = catalog.load_table("hydrofabric.flowpath-attributes-ml")
        valid_flowpath_ids = filtered_flowpaths["id"].dropna().values.tolist()
        filtered_flowpath_attr_ml = flowpath_attr_ml.scan(row_filter=In("id", valid_flowpath_ids)).to_pandas()
        output_layers["flowpath-attributes-ml"] = (filtered_flowpath_attr_ml,)

    if "pois" in layers:
        pois = catalog.load_table("hydrofabric.pois")
        hydrolocations = catalog.load_table("hydrofabric.hydrolocations")
        filtered_poi_list = []
        if len(filtered_flowpaths) > 0:
            poi_values = filtered_flowpaths["poi_id"].dropna().values
            filtered_poi_list = list({int(x) for x in poi_values if pd.notna(x)})
            filtered_pois = pois.scan(row_filter=In("poi_id", filtered_poi_list)).to_pandas()
            filtered_hydrolocations = hydrolocations.scan(
                row_filter=In("poi_id", filtered_poi_list)
            ).to_pandas()
        else:
            filtered_pois = pd.DataFrame()
            filtered_hydrolocations = pd.DataFrame()
        output_layers["pois"] = filtered_pois
        output_layers["hydrolocations"] = filtered_hydrolocations

    if output_file:
        for table_name, _layer in output_layers.items():
            if len(_layer) > 0:  # Only save non-empty layers
                gpd.GeoDataFrame(_layer).to_file(output_file, layer=table_name, driver="GPKG")
            else:
                print(f"Warning: {table_name} layer is empty")
    else:
        return output_layers


# if __name__ == "__main__":
#     warehouse_path = "/tmp/warehouse"  # Requires the HF to be built via: src/icefabric_manage/builds/build_hydrofabric.py
#     catalog_settings = {
#         "type": "sql",
#         "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
#         "warehouse": f"file://{warehouse_path}",
#     }
#     hydrofabric_catalog = load_catalog("hydrofabric", **catalog_settings)
#     subset(
#         catalog=hydrofabric_catalog,
#         identifier="gages-01010000",
#         id_type=IdType.HL_URI,
#         output_file="subset.gpkg",
#     )
