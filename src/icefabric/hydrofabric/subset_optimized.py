"""Functional hydrofabric subset implementation using pre-computed upstream lookup table with Polars"""

import json
from pathlib import Path

import polars as pl
import zarr
from pyiceberg.catalog import Catalog, load_catalog

from icefabric.schemas.hydrofabric import HydrofabricDomains, IdType


def preprocess_river_network(network: pl.LazyFrame) -> dict[str, list[str]]:
    """
    Preprocesses the network to find DIRECT connections only

    This is a helper function that finds immediate upstream connections.
    It's used by build_upstream_lookup() as a starting point.

    Parameters
    ----------
    network: pl.LazyFrame
        Network table

    Returns
    -------
    dict[str, list[str]]
        A dictionary mapping downstream segments to their DIRECT upstream values
    """
    network_dict = (
        network.filter(pl.col("toid").is_not_null())
        .group_by("toid")
        .agg(pl.col("id").alias("upstream_ids"))
        .collect()
    )

    # Create a lookup for nexus -> downstream wb connections
    nexus_downstream = (
        network.filter(pl.col("id").str.starts_with("nex-"))
        .filter(pl.col("toid").str.starts_with("wb-"))
        .select(["id", "toid"])
        .rename({"id": "nexus_id", "toid": "downstream_wb"})
    ).collect()

    # Explode the upstream_ids to get one row per connection
    connections = network_dict.with_row_index().explode("upstream_ids")

    # Separate wb-to-wb connections (keep as-is)
    wb_to_wb = (
        connections.filter(pl.col("upstream_ids").str.starts_with("wb-"))
        .filter(pl.col("toid").str.starts_with("wb-"))
        .select(["toid", "upstream_ids"])
    )

    # Handle nexus connections: wb -> nex -> wb becomes wb -> wb
    wb_to_nexus = (
        connections.filter(pl.col("upstream_ids").str.starts_with("wb-"))
        .filter(pl.col("toid").str.starts_with("nex-"))
        .join(nexus_downstream, left_on="toid", right_on="nexus_id", how="inner")
        .select(["downstream_wb", "upstream_ids"])
        .rename({"downstream_wb": "toid"})
    )

    # Combine both types of connections
    wb_connections = pl.concat([wb_to_wb, wb_to_nexus]).unique()

    # Group back to dictionary format
    wb_network_result = wb_connections.group_by("toid").agg(pl.col("upstream_ids")).unique()
    wb_network_dict = {row["toid"]: row["upstream_ids"] for row in wb_network_result.iter_rows(named=True)}
    return wb_network_dict


def save_upstream_lookup(
    lookup_df: pl.DataFrame,
    storage_type: str = "parquet",
    path: Path | None = None,
    catalog: Catalog | None = None,
) -> Path:
    """
    Save the upstream lookup table using different storage options

    Parameters
    ----------
    lookup_df : pl.DataFrame
        The upstream lookup table
    storage_type : str
        Storage type: 'parquet', 'zarr', or 'iceberg'
    path : Path, optional
        Storage path (required for parquet and zarr)
    catalog : Catalog, optional
        Iceberg catalog (required for iceberg storage)

    Returns
    -------
    Path
        Path where the table was saved
    """
    if storage_type == "parquet":
        if path is None:
            path = Path("upstream_lookup.parquet")
        lookup_df.write_parquet(path)
        print(f"Saved upstream lookup to parquet: {path}")
        return path

    elif storage_type == "zarr":
        if path is None:
            path = Path("upstream_lookup.zarr")

        # Convert to zarr format
        store = zarr.storage.LocalStore(root=path)
        root = zarr.create_group(store=store)

        # Store flowpath IDs
        flowpath_ids = lookup_df.get_column("flowpath_id").to_numpy()
        root.create_array("flowpath_ids", data=flowpath_ids, dtype="U50")

        # Store upstream data as JSON strings (to handle ragged arrays)
        upstream_data = [
            json.dumps(upstream_list) if upstream_list else "[]"
            for upstream_list in lookup_df.get_column("upstream_flowpaths").to_list()
        ]
        root.create_array("upstream_json", data=upstream_data, dtype="U10000")

        # Store metadata
        root.attrs["format"] = "upstream_lookup"
        root.attrs["version"] = "1.0"
        root.attrs["description"] = "Pre-computed upstream flowpath lookup table"

        print(f"Saved upstream lookup to zarr: {path}")
        return path

    elif storage_type == "iceberg":
        if catalog is None:
            raise ValueError("Catalog required for iceberg storage")

        # Convert list column to string for iceberg compatibility
        iceberg_df = lookup_df.with_columns(
            pl.col("upstream_flowpaths")
            .map_elements(lambda x: json.dumps(x), return_dtype=pl.String)
            .alias("upstream_flowpaths_json")
        ).drop("upstream_flowpaths")

        # Convert to arrow table
        arrow_table = iceberg_df.to_arrow()

        # Create or replace iceberg table
        table_name = "hydrofabric.upstream_lookup"
        try:
            # Try to load existing table and replace data
            table = catalog.load_table(table_name)
            table.delete()
            table.append(arrow_table)
        except:
            # Create new table
            table = catalog.create_table(table_name, schema=arrow_table.schema)
            table.append(arrow_table)

        print(f"Saved upstream lookup to iceberg table: {table_name}")
        return Path(table_name)

    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")


def load_upstream_lookup(
    storage_type: str = "parquet", path: Path | None = None, catalog: Catalog | None = None
) -> pl.DataFrame:
    """
    Load the upstream lookup table from storage

    Parameters
    ----------
    storage_type : str
        Storage type: 'parquet', 'zarr', or 'iceberg'
    path : Path, optional
        Storage path (required for parquet and zarr)
    catalog : Catalog, optional
        Iceberg catalog (required for iceberg storage)

    Returns
    -------
    pl.DataFrame
        The loaded upstream lookup table
    """
    if path is None:
        path = Path(__file__).parents[3] / "data/upstream_lookup.zarr"

        root = zarr.open_group(store=path)
        flowpath_ids = root["flowpath_ids"][:]
        upstream_json = root["upstream_json"][:]

        # Parse JSON back to lists
        upstream_lists = [json.loads(json_str) for json_str in upstream_json]

        return pl.DataFrame({"flowpath_id": flowpath_ids, "upstream_flowpaths": upstream_lists})

    elif storage_type == "iceberg":
        if catalog is None:
            raise ValueError("Catalog required for iceberg storage")

        table = catalog.load_table("hydrofabric.upstream_lookup")
        arrow_data = table.scan().to_arrow()
        df = pl.from_arrow(arrow_data)

        # Convert JSON string back to list
        return df.with_columns(
            pl.col("upstream_flowpaths_json")
            .map_elements(lambda x: json.loads(x), return_dtype=pl.List(pl.String))
            .alias("upstream_flowpaths")
        ).drop("upstream_flowpaths_json")

    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")


def create_upstream_lookup_dict(lookup_df: pl.DataFrame) -> dict[str, set[str]]:
    """
    Convert upstream lookup DataFrame to a fast dictionary

    Parameters
    ----------
    lookup_df : pl.DataFrame
        The upstream lookup table

    Returns
    -------
    Dict[str, Set[str]]
        Dictionary mapping flowpath_id -> set of upstream flowpath IDs
    """
    upstream_dict = {}
    for row in lookup_df.iter_rows(named=True):
        upstream_dict[row["flowpath_id"]] = set(row["upstream_flowpaths"])
    return upstream_dict


def get_upstream_segments(origin_id: str, upstream_dict: dict[str, set[str]]) -> set[str]:
    """
    Get all upstream segments for a given origin

    Parameters
    ----------
    origin_id : str
        The origin flowpath ID
    upstream_dict : Dict[str, Set[str]]
        Dictionary mapping flowpath_id -> set of upstream flowpath IDs

    Returns
    -------
    Set[str]
        Set of all upstream flowpath IDs including the origin
    """
    if origin_id not in upstream_dict:
        print(f"Warning: {origin_id} not found in upstream lookup")
        return {origin_id}

    upstream_set = upstream_dict[origin_id].copy()
    upstream_set.add(origin_id)  # Include the origin itself
    return upstream_set


def find_origin_flowpath(catalog: Catalog, identifier: str, id_type: str) -> str:
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
    network_table = catalog.load_table("hydrofabric.network")
    network_df = pl.from_arrow(network_table.scan().to_arrow()).lazy()

    # Find matching network entries
    if id_type == "hl_uri":
        matches = network_df.filter(pl.col("hl_uri") == identifier).select("id").collect()
    else:
        raise ValueError(f"ID type {id_type} not yet implemented")

    if matches.height == 0:
        raise ValueError(f"No network entry found for {identifier}")

    network_ids = matches.get_column("id").to_list()

    if len(network_ids) == 1:
        return network_ids[0]
    else:
        # Multiple matches - find best one based on drainage area
        flowpaths_table = catalog.load_table("hydrofabric.flowpaths")
        flowpaths_df = pl.from_arrow(flowpaths_table.scan().to_arrow()).lazy()

        candidates = (
            flowpaths_df.filter(pl.col("id").is_in(network_ids))
            .select(["id", "tot_drainage_areasqkm"])
            .collect()
        )

        # Return the first match for now
        # TODO: Implement proper drainage area comparison
        return candidates.get_column("id")[0]


def subset_layer_polars(catalog: Catalog, layer_name: str, upstream_ids: set[str]) -> pl.LazyFrame:
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
    try:
        table = catalog.load_table(f"hydrofabric.{layer_name}")

        # Convert to polars lazy frame
        arrow_dataset = table.scan().to_arrow()
        lazy_frame = pl.from_arrow(arrow_dataset).lazy()

        # Determine the ID column name based on layer type
        id_column_map = {
            "flowpaths": "id",
            "flowpath-attributes": "id",
            "flowpath-attributes-ml": "id",
            "divides": "divide_id",
            "divide-attributes": "divide_id",
            "network": "id",
            "nexus": "id",
            "lakes": "id",
            "hydrolocations": "id",
            "pois": "id",
        }

        id_column = id_column_map.get(layer_name, "id")

        # Convert set to list for polars
        upstream_list = list(upstream_ids)

        # Filter efficiently using polars
        filtered = lazy_frame.filter(pl.col(id_column).is_in(upstream_list))

        return filtered

    except Exception as e:
        print(f"Warning: Could not subset layer {layer_name}: {e}")
        return pl.LazyFrame()


def create_or_load_upstream_lookup(
    catalog: Catalog,
    storage_type: str = "iceberg",
    lookup_path: Path | None = None,
    force_rebuild: bool = False,
) -> pl.DataFrame:
    """
    Create or load the upstream lookup table

    Parameters
    ----------
    catalog : Catalog
        PyIceberg catalog
    storage_type : str
        Storage type: 'parquet', 'zarr', or 'iceberg'
    lookup_path : Path, optional
        Path for storage
    force_rebuild : bool
        Force rebuilding even if lookup exists

    Returns
    -------
    pl.DataFrame
        The upstream lookup table
    """
    # Check if lookup already exists
    if not force_rebuild:
        try:
            lookup_df = load_upstream_lookup(storage_type=storage_type, path=lookup_path, catalog=catalog)
            print("Loaded existing upstream lookup table")
            return lookup_df
        except:
            print("No existing upstream lookup found, will create new one")

    print("Building new upstream lookup table...")

    # Load required tables
    fp_table = catalog.load_table("hydrofabric.flowpaths").to_polars()
    network_table = catalog.load_table("hydrofabric.network").to_polars()

    # Build lookup table
    lookup_df = preprocess_river_network(fp_table)

    # Save for future use
    save_upstream_lookup(lookup_df, storage_type=storage_type, path=lookup_path, catalog=catalog)

    return lookup_df


def subset_hydrofabric(
    catalog: Catalog,
    identifier: str,
    id_type: str,
    layers: list[str],
    domain: str,
    upstream_dict: dict[str, set[str]],
) -> dict[str, pl.LazyFrame]:
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
    print(f"Starting optimized subset for {identifier}")

    # Step 1: Find the origin flowpath
    origin_id = find_origin_flowpath(catalog, identifier, id_type)
    print(f"Found origin flowpath: {origin_id}")

    # Step 2: Get upstream segments from pre-computed lookup
    upstream_ids = get_upstream_segments(origin_id, upstream_dict)
    print(f"Found {len(upstream_ids)} upstream segments")

    # Step 3: Subset each layer efficiently using Polars
    result = {}
    for layer in layers:
        print(f"Subsetting layer: {layer}")
        try:
            subset_layer = subset_layer_polars(catalog, layer, upstream_ids)
            result[layer] = subset_layer
        except Exception as e:
            print(f"Failed to subset layer {layer}: {e}")
            continue

    return result


def subset_v2(
    catalog: Catalog,
    identifier: str,
    id_type: str,
    layers: list[str],
    output_file: Path,
    domain: str,
    lookup_storage: str = "iceberg",
    lookup_path: Path | None = None,
) -> None:
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
    domain : str
        Domain name
    lookup_storage : str
        Storage type for upstream lookup table
    lookup_path : Path, optional
        Path to upstream lookup table
    """
    # Create or load upstream lookup table
    upstream_lookup = create_or_load_upstream_lookup(
        catalog=catalog, storage_type=lookup_storage, lookup_path=lookup_path
    )

    # Convert to fast lookup dictionary
    upstream_dict = create_upstream_lookup_dict(upstream_lookup)
    print(f"Loaded upstream lookup for {len(upstream_dict)} flowpaths")

    # Perform subset
    subsetted_layers = subset_hydrofabric(
        catalog=catalog,
        identifier=identifier,
        id_type=id_type,
        layers=layers,
        domain=domain,
        upstream_dict=upstream_dict,
    )

    # Write results
    output_file.parent.mkdir(parents=True, exist_ok=True)

    for layer_name, layer_data in subsetted_layers.items():
        try:
            # Collect the lazy frame and save
            collected = layer_data.collect()
            if collected.height > 0:
                output_path = output_file.parent / f"{identifier}_{layer_name}.parquet"
                collected.write_parquet(output_path)
                print(f"Saved {layer_name}: {collected.height} rows to {output_path}")
            else:
                print(f"No data found for layer {layer_name}")
        except Exception as e:
            print(f"Failed to save {layer_name}: {e}")


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
