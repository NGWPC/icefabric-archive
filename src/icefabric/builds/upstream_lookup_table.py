"""A file containing code to build an upstream lookup table"""

import json
from pathlib import Path

import polars as pl
from pyiceberg.catalog import Catalog


def _build_upstream_lookup_table(network_table):
    network_dict = (
        network_table.filter(pl.col("toid").is_not_null())
        .group_by("toid")
        .agg(pl.col("id").alias("upstream_ids"))
        .collect()
    )
    # Create a lookup for nexus -> downstream wb connections
    nexus_downstream = (
        network_table.filter(pl.col("id").str.starts_with("nex-"))
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


def build_upstream_json(catalog: Catalog, output_path: Path):
    """Build upstream lookup table and save to JSON file"""
    network_table = catalog.load_table("hydrofabric.network").to_polars()
    wb_network_dict = _build_upstream_lookup_table(network_table)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(wb_network_dict, f, indent=2)

    print(f"Upstream lookup table saved to {output_path}")
