"""Contains helper functions to support NWM modules"""

import json
from pathlib import Path

from .hydrofabric import UPSTREAM_VPUS, HydrofabricDomains, IdType
from .iceberg_tables.ras_xs import ExtractedRasXS
from .modules import (
    LASAM,
    LSTM,
    SFT,
    SMP,
    Albedo,
    CalibratableScheme,
    IceFractionScheme,
    NoahOwpModular,
    SacSma,
    SacSmaValues,
    Snow17,
    SoilScheme,
    Topmodel,
    Topoflow,
    TRoute,
)
from .ras_xs import XsType
from .topobathy import FileType, NGWPCLocations, NGWPCTestLocations


def load_upstream_connections(domain_val):
    """Helper function to generate the upstream_dict used in parameter generation"""
    try:
        # Load upstream connections (same as CLI)
        upstream_connections_path = (
            Path(__file__).parents[3] / f"data/hydrofabric/{domain_val}_upstream_connections.json"
        )

        if not upstream_connections_path.exists():
            raise FileNotFoundError(
                f"Upstream connections missing for {domain_val}. Please run `icefabric build-upstream-connections` to generate this file"
            )

        with open(upstream_connections_path) as f:
            data = json.load(f)
            print(
                f"Loading upstream connections generated on: {data['_metadata']['generated_at']} "
                f"from snapshot id: {data['_metadata']['iceberg']['snapshot_id']}"
            )
            upstream_dict = data["upstream_connections"]
    except FileNotFoundError:
        raise
    return upstream_dict


__all__ = [
    "ExtractedRasXS",
    "UPSTREAM_VPUS",
    "IdType",
    "HydrofabricDomains",
    "SFT",
    "IceFractionScheme",
    "Albedo",
    "Snow17",
    "CalibratableScheme",
    "SMP",
    "SoilScheme",
    "SacSma",
    "SacSmaValues",
    "LSTM",
    "LASAM",
    "NoahOwpModular",
    "TRoute",
    "Topmodel",
    "Topoflow",
    "FileType",
    "NGWPCLocations",
    "NGWPCTestLocations",
    "XsType",
]
