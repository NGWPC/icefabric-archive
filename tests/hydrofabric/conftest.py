"""
Mock PyIceberg Catalog Test Suite for Hydrofabric v2.2 Data
"""

import json
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo, In

# Sample upstream connections data structure (Hawaii subset)
SAMPLE_UPSTREAM_CONNECTIONS = {
    "_metadata": {
        "generated_at": "2025-07-21T18:45:57.169868+00:00",
        "iceberg": {
            "catalog_name": "mock_glue",
            "source_table": "mock_hf.network",
            "snapshot_id": 1464906773857472435,
        },
    },
    "upstream_connections": {
        "wb-2813": ["wb-2896"],
        "wb-2815": ["wb-2931"],
        "wb-1421": ["wb-1775"],
        "wb-3019": ["wb-3126", "wb-3270"],
        "wb-1496": ["wb-1495"],
        "wb-1425": ["wb-1424"],
        "wb-2034": ["wb-2072"],
        "wb-3042": ["wb-3075"],
        "wb-1890": ["wb-1889"],
        "wb-2843": ["wb-2842", "wb-2927"],
        "wb-2437": ["wb-2467"],
        "wb-1914": ["wb-1913", "wb-1926"],
        "wb-1962": ["wb-1961"],
        "wb-2426": ["wb-2425", "wb-2483"],
        "wb-1966": ["wb-1973"],
        "wb-1559": ["wb-1641"],
        "wb-1952": ["wb-1956"],
        "wb-2738": ["wb-2737"],
        "wb-3051": ["wb-3207"],
        "wb-1971": ["wb-1970", "wb-2071"],
        "wb-1917": ["wb-2056"],
        "wb-1506": ["wb-1505"],
        "wb-2402": ["wb-2401"],
        "wb-1391": ["wb-1390"],
        "wb-1399": ["wb-1769"],
        "wb-2195": ["wb-2225"],
        "wb-1432": ["wb-1431", "wb-1771", "wb-1675"],
        "wb-2700": ["wb-2741", "wb-2699", "wb-2813"],
        "wb-1618": ["wb-1727", "wb-1617"],
        "wb-1365": ["wb-1364", "wb-1519"],
        "wb-1653": ["wb-1718"],
        "wb-1466": ["wb-1666"],
        "wb-2717": ["wb-2758"],
        "wb-2753": ["wb-2752"],
        "wb-3052": ["wb-3051"],
        "wb-1928": ["wb-1953"],
        "wb-1363": ["wb-1469", "wb-1362"],
        "wb-2736": ["wb-2735"],
        "wb-2828": ["wb-2827"],
        "wb-3151": ["wb-3150", "wb-3272"],
        "wb-2218": ["wb-2311"],
        "wb-2651": ["wb-2698"],
        "wb-1679": ["wb-1678"],
        "wb-2720": ["wb-2719"],
        "wb-1899": ["wb-1898"],
        "wb-1858": ["wb-1863"],
        "wb-1487": ["wb-1760"],
        "wb-2706": ["wb-2705", "wb-2890"],
        "wb-2708": ["wb-2707"],
        "wb-2207": ["wb-2206", "wb-2310"],
        "wb-2643": ["wb-2943", "wb-2642"],
        "wb-1593": ["wb-1634"],
        "wb-1926": ["wb-1925"],
        "wb-1415": ["wb-1576"],
        "wb-2640": ["wb-2639"],
        "wb-1451": ["wb-1450"],
        "wb-1993": ["wb-2038"],
        "wb-1397": ["wb-1396"],
        "wb-2751": ["wb-2750"],
        "wb-1861": ["wb-1866"],
        "wb-2660": ["wb-2659", "wb-2954"],
        "wb-3063": ["wb-3070", "wb-3062"],
    },
}


class MockTable:
    """Mock PyIceberg Table with realistic Hawaii hydrofabric data"""

    def __init__(self, table_name: str, data: pd.DataFrame):
        self.table_name = table_name
        self.data = data
        self._polars_data = pl.from_pandas(data).lazy()

    def scan(self, row_filter=None):
        """Mock scan method that applies filters"""
        return MockScan(self._polars_data, row_filter)

    def to_polars(self) -> pl.LazyFrame:
        """Returns data as Polars DataFrame"""
        return self._polars_data


class MockScan:
    """Mock scan result that can be filtered and converted"""

    def __init__(self, data: pl.LazyFrame, row_filter=None):
        self.data = data
        self.row_filter = row_filter

    def _apply_filters(self) -> pl.DataFrame:
        """Apply filters to the data and return filtered Polars DataFrame"""
        if self.row_filter is None:
            return self.data.collect()

        # Handle different filter types
        if isinstance(self.row_filter, EqualTo):
            column_name = self.row_filter.term.name
            value = self.row_filter.literal.value
            return self.data.filter(pl.col(column_name) == value).collect()
        elif isinstance(self.row_filter, In):
            column_name = self.row_filter.term.name
            values = [lit.value for lit in self.row_filter.literals]
            return self.data.filter(pl.col(column_name).is_in(values)).collect()

        return self.data.collect()

    def to_polars(self) -> pl.DataFrame:
        """Apply filters and returns a Polars DataFrame"""
        return self._apply_filters()

    def to_pandas(self) -> pd.DataFrame:
        """Apply filters and returns a Pandas DataFrame"""
        return self._apply_filters().to_pandas()

    def to_arrow(self) -> pa.Table:
        """Returns data as arrow table"""
        return pa.Table.from_pandas()


class MockCatalog:
    """Mock PyIceberg Catalog with sample Hawaii hydrofabric data"""

    def __init__(self, catalog_type: str = "glue"):
        self.catalog_type = catalog_type
        self.tables = self._create_sample_tables()

    def load_table(self, table_name: str) -> MockTable:
        """Load a mock table by name"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")
        return self.tables[table_name]

    def _create_sample_tables(self) -> dict[str, MockTable]:
        """Create sample hydrofabric tables with realistic data"""
        tables = {}
        # Network table - core connectivity data
        network_data = self._create_network_data()
        tables["mock_hf.network"] = MockTable("mock_hf.network", network_data)

        # Flowpaths table - stream geometry
        flowpaths_data = self._create_flowpaths_data(network_data)
        tables["mock_hf.flowpaths"] = MockTable("mock_hf.flowpaths", flowpaths_data)

        # Nexus table - connection points
        nexus_data = self._create_nexus_data(network_data)
        tables["mock_hf.nexus"] = MockTable("mock_hf.nexus", nexus_data)

        # Divides table - watershed boundaries
        divides_data = self._create_divides_data(network_data)
        tables["mock_hf.divides"] = MockTable("mock_hf.divides", divides_data)

        # Lakes table
        lakes_data = self._create_lakes_data()
        tables["mock_hf.lakes"] = MockTable("mock_hf.lakes", lakes_data)

        # Attribute tables
        tables["mock_hf.divide-attributes"] = MockTable(
            "mock_hf.divide-attributes", self._create_divide_attributes(divides_data)
        )
        tables["mock_hf.flowpath-attributes"] = MockTable(
            "mock_hf.flowpath-attributes", self._create_flowpath_attributes(flowpaths_data)
        )
        tables["mock_hf.flowpath-attributes-ml"] = MockTable(
            "mock_hf.flowpath-attributes-ml", self._create_flowpath_attributes_ml()
        )
        tables["mock_hf.pois"] = MockTable("mock_hf.pois", self._create_pois_data(network_data))
        tables["mock_hf.hydrolocations"] = MockTable(
            "mock_hf.hydrolocations", self._create_hydrolocations_data()
        )

        return tables

    def _create_network_data(self) -> pd.DataFrame:
        """Create sample network connectivity data"""
        upstream_connections = SAMPLE_UPSTREAM_CONNECTIONS["upstream_connections"]

        network_data = []
        poi_counter = 0
        hydroseq_counter = 1

        # Collect all unique watershed IDs
        all_wb_ids = set()
        for parent, children in upstream_connections.items():
            all_wb_ids.add(parent)
            for child in children:
                all_wb_ids.add(child)

        # Create flowpath records for each watershed (wb-*)
        for wb_id in all_wb_ids:
            wb_num = int(wb_id.split("-")[1])

            # Determine the toid (nexus point this watershed flows to)
            toid = f"nex-{wb_num}"  # Each watershed flows to its corresponding nexus

            # Some records have POIs
            has_poi = poi_counter % 10 == 0  # Every 4th watershed has a POI
            poi_id = float(poi_counter) if has_poi else None
            poi_counter += 1

            # Create the flowpath record
            record = {
                "id": wb_id,
                "toid": toid,
                "divide_id": f"cat-{wb_num}",
                "ds_id": None,
                "mainstem": None,
                "poi_id": poi_id,
                "hydroseq": float(hydroseq_counter),
                "hf_source": "NHDPlusHR",
                "hf_id": str(wb_num),
                "lengthkm": round(0.5 + (wb_num % 100) / 10.0, 2),
                "areasqkm": round(1.0 + (wb_num % 500) / 10.0, 2),
                "tot_drainage_areasqkm": round(10.0 + (wb_num % 5000) / 10.0, 2),
                "type": "waterbody",
                "vpuid": "hi",
                "topo": "fl-nex",  # All records in Hawaii are fl-nex
                "hl_uri": f"gages-{wb_num:06d}" if has_poi else None,
            }

            network_data.append(record)
            hydroseq_counter += 1

        # Create nexus records that connect upstream to downstream
        for parent, children in upstream_connections.items():
            # Create nexus points for each child that flows to the parent
            for child in children:
                child_num = int(child.split("-")[1])

                # The nexus point for this child flows to the parent watershed
                nexus_record = {
                    "id": f"nex-{child_num}",  # Nexus point for the child
                    "toid": parent,  # Points to parent watershed
                    "divide_id": f"cat-{child_num}",
                    "ds_id": None,
                    "mainstem": None,
                    "poi_id": None,
                    "hydroseq": float(hydroseq_counter),
                    "hf_source": "NHDPlusHR",
                    "hf_id": str(child_num),
                    "lengthkm": 0.01,  # Nexus points have minimal length
                    "areasqkm": 0.001,
                    "tot_drainage_areasqkm": 0.01,
                    "type": "nexus",
                    "vpuid": "hi",
                    "topo": "fl-nex",
                    "hl_uri": None,  # Nexus points don't have hl_uri
                }

                network_data.append(nexus_record)
                hydroseq_counter += 1

        # Create outlet nexus points for watersheds that don't flow to other watersheds
        outlet_watersheds = set()
        upstream_watersheds = set()

        for parent, children in upstream_connections.items():
            outlet_watersheds.add(parent)
            for child in children:
                upstream_watersheds.add(child)

        # True outlets are watersheds that are not upstream of anything
        true_outlets = outlet_watersheds - upstream_watersheds

        for outlet_wb in true_outlets:
            outlet_num = int(outlet_wb.split("-")[1])

            # Create nexus point for outlet that flows to coastal connection
            outlet_nexus_record = {
                "id": f"nex-{outlet_num}",
                "toid": f"cnx-{outlet_num % 100}",  # Points to coastal nexus (can overlap)
                "divide_id": f"cat-{outlet_num}",
                "ds_id": None,
                "mainstem": None,
                "poi_id": None,
                "hydroseq": float(hydroseq_counter),
                "hf_source": "NHDPlusHR",
                "hf_id": str(outlet_num),
                "lengthkm": 0.01,
                "areasqkm": 0.001,
                "tot_drainage_areasqkm": 0.01,
                "type": "nexus",
                "vpuid": "hi",
                "topo": "fl-nex",
                "hl_uri": None,
            }

            network_data.append(outlet_nexus_record)
            hydroseq_counter += 1

        # Add terminal nexus points (tnx-) for flowlines that are ending
        # These represent the actual terminus of flowlines
        for outlet_wb in true_outlets:
            outlet_num = int(outlet_wb.split("-")[1])

            terminal_nexus_record = {
                "id": f"tnx-{1000000000 + outlet_num}",  # Terminal nexus with large ID
                "toid": f"cnx-{(outlet_num % 50) + 1}",  # Points to coastal, can overlap
                "divide_id": f"cat-{outlet_num}",
                "ds_id": None,
                "mainstem": None,
                "poi_id": None,
                "hydroseq": float(hydroseq_counter),
                "hf_source": "NHDPlusHR",
                "hf_id": str(1000000000 + outlet_num),
                "lengthkm": 0.001,  # Very small for terminal points
                "areasqkm": 0.0001,
                "tot_drainage_areasqkm": 0.001,
                "type": "terminal",
                "vpuid": "hi",
                "topo": "fl-nex",
                "hl_uri": None,
            }

            network_data.append(terminal_nexus_record)
            hydroseq_counter += 1

        # Add coastal nexus points (records with null ID) - these are the final outlets
        for i in range(1, 51):  # Add 50 coastal outlets (CNX points can be shared)
            record = {
                "id": None,  # Null ID for coastal nexus
                "toid": f"cnx-{i}",
                "divide_id": f"cat-coastal-{i}",
                "ds_id": None,
                "mainstem": None,
                "poi_id": None,
                "hydroseq": None,
                "hf_source": None,
                "hf_id": None,
                "lengthkm": None,
                "areasqkm": None,
                "tot_drainage_areasqkm": None,
                "type": "coastal",
                "vpuid": "hi",
                "topo": "fl-nex",
                "hl_uri": None,
            }
            network_data.append(record)

        return pd.DataFrame(network_data)

    def _create_flowpaths_data(self, network_df: pd.DataFrame) -> pd.DataFrame:
        """Create sample flowpath geometry data"""
        flowpaths = []

        upstream_connections = SAMPLE_UPSTREAM_CONNECTIONS["upstream_connections"]

        # Collect all unique watershed IDs
        all_wb_ids = set()
        for parent, children in upstream_connections.items():
            all_wb_ids.add(parent)
            for child in children:
                all_wb_ids.add(child)

        wb_records = network_df[network_df["id"].isin(all_wb_ids)]

        for _, row in wb_records.iterrows():
            # Create a simple LineString geometry as binary (matching real schema)
            geometry_binary = b"\x01\x01\x00\x00\x00fl#\xd5g\xaf\x13\xc1\x96!\x8e\xa5\xfe\x14.A"

            flowpaths.append(
                {
                    "id": row["id"],
                    "toid": row["toid"],
                    "mainstem": None,  # DoubleType
                    "order": float(hash(row["id"]) % 7 + 1),  # DoubleType, stream order 1-7
                    "hydroseq": int(row["hydroseq"]) if row["hydroseq"] else None,  # IntegerType
                    "lengthkm": row["lengthkm"],  # DoubleType
                    "areasqkm": row["areasqkm"],  # DoubleType
                    "tot_drainage_areasqkm": row["tot_drainage_areasqkm"],  # DoubleType
                    "has_divide": True,  # BooleanType
                    "divide_id": row["divide_id"],  # StringType
                    "poi_id": str(int(row["poi_id"]))
                    if pd.isna(row["poi_id"]) is False
                    else pd.NA,  # StringType
                    "vpuid": "hi",  # StringType
                    "geometry": geometry_binary,  # BinaryType
                }
            )

        return pd.DataFrame(flowpaths)

    def _create_nexus_data(self, network_df: pd.DataFrame) -> pd.DataFrame:
        """Create sample nexus point data matching Hawaii schema"""
        nexus_points = []

        # Create nexus points for toids and add some coastal nexus points
        unique_toids = network_df["toid"].dropna().unique()

        counter = 1
        for toid in unique_toids:
            if toid and not toid.endswith("_downstream"):
                # Create mock binary geometry for Point (copying from a HY Nexus)
                geometry_binary = b"\x01\x01\x00\x00\x00fl#\xd5g\xaf\x13\xc1\x96!\x8e\xa5\xfe\x14.A"

                # Determine type and poi_id
                if toid.startswith("cnx-"):
                    nexus_type = "coastal"
                    poi_id = None
                elif toid.startswith("nex-"):
                    nexus_type = "network"
                    poi_id = str(counter) if counter % 3 == 0 else None
                else:
                    nexus_type = "terminal"
                    poi_id = None

                nexus_points.append(
                    {
                        "id": toid,
                        "toid": "wb-0" if nexus_type == "coastal" else None,  # Coastal points flow to wb-0
                        "poi_id": poi_id,  # StringType
                        "type": nexus_type,  # StringType
                        "vpuid": "hi",  # StringType
                        "geometry": geometry_binary,  # BinaryType
                    }
                )
                counter += 1

        # Add a few specific coastal nexus points (like in real data)
        for i in range(1, 6):
            geometry_binary = b"\x01\x01\x00\x00\x00fl#\xd5g\xaf\x13\xc1\x96!\x8e\xa5\xfe\x14.A"

            nexus_points.append(
                {
                    "id": f"cnx-{i}",
                    "toid": "wb-0",
                    "poi_id": None,
                    "type": "coastal",
                    "vpuid": "hi",
                    "geometry": geometry_binary,
                }
            )

        return pd.DataFrame(nexus_points)

    def _create_divides_data(self, network_df: pd.DataFrame) -> pd.DataFrame:
        """Create sample watershed divide geometry data matching Hawaii schema"""
        divides = []
        upstream_connections = SAMPLE_UPSTREAM_CONNECTIONS["upstream_connections"]

        all_wb_ids = set()
        for parent, children in upstream_connections.items():
            all_wb_ids.add(parent)
            for child in children:
                all_wb_ids.add(child)

        wb_records = network_df[network_df["id"].isin(all_wb_ids)]

        for _, row in wb_records.iterrows():
            # Create a simple polygon for the watershed boundary as binary
            geometry_binary = b"\x01\x01\x00\x00\x00fl#\xd5g\xaf\x13\xc1\x96!\x8e\xa5\xfe\x14.A"

            # Determine toid and type
            if row["toid"] and row["toid"].startswith("cnx-"):
                divide_type = "terminal"
                toid = f"tnx-{hash(row['id']) % 1000000}"  # Terminal nexus
            else:
                divide_type = "network"
                toid = row["toid"]

            divides.append(
                {
                    "divide_id": row["divide_id"],
                    "toid": toid,
                    "type": divide_type,  # StringType
                    "ds_id": None,  # DoubleType
                    "areasqkm": row["areasqkm"],  # DoubleType
                    "id": row["id"],  # StringType
                    "lengthkm": row["lengthkm"],  # DoubleType
                    "tot_drainage_areasqkm": row["tot_drainage_areasqkm"],  # DoubleType
                    "has_flowline": True,  # BooleanType
                    "vpuid": "hi",  # StringType
                    "geometry": geometry_binary,  # BinaryType
                }
            )

        return pd.DataFrame(divides)

    def _create_lakes_data(self) -> pd.DataFrame:
        """Create sample lakes data matching Hawaii schema"""
        lakes = []

        # Create a few lakes with realistic Hawaii schema
        for i in range(5, 10):  # Create 5 lakes
            x = -100.0 + i * 0.02
            y = 40.0 + i * 0.02

            # Create mock binary geometry for Point (lake centroid)
            geometry_binary = b"\x01\x01\x00\x00\x00fl#\xd5g\xaf\x13\xc1\x96!\x8e\xa5\xfe\x14.A"

            lakes.append(
                {
                    "lake_id": float(800020000 + i),  # DoubleType
                    "LkArea": round(0.1 + i * 0.3, 2),  # DoubleType
                    "LkMxE": round(90.0 + i * 50, 2),  # DoubleType
                    "WeirC": 2.6,  # DoubleType
                    "WeirL": 10.0,  # DoubleType
                    "OrificeC": 0.6,  # DoubleType
                    "OrificeA": 1.0,  # DoubleType
                    "OrificeE": 10.0,  # DoubleType
                    "WeirE": 15.0,  # DoubleType
                    "ifd": 0.9,  # DoubleType
                    "Dam_Length": 100.0,  # DoubleType
                    "domain": "hi",  # StringType
                    "poi_id": i + 1000,  # IntegerType
                    "hf_id": float(8000010000000 + i),  # DoubleType
                    "reservoir_index_AnA": None,  # DoubleType
                    "reservoir_index_Extended_AnA": None,  # DoubleType
                    "reservoir_index_GDL_AK": None,  # DoubleType
                    "reservoir_index_Medium_Range": None,  # DoubleType
                    "reservoir_index_Short_Range": None,  # DoubleType
                    "res_id": f"res-{800020000 + i}",  # StringType
                    "vpuid": "hi",  # StringType
                    "lake_x": x,  # DoubleType
                    "lake_y": y,  # DoubleType
                    "geometry": geometry_binary,  # BinaryType
                }
            )
        return pd.DataFrame(lakes)

    def _create_divide_attributes(self, divides_df: pd.DataFrame) -> pd.DataFrame:
        """Create sample divide attributes data matching Hawaii schema"""

        attributes = []
        for _, row in divides_df.iterrows():
            # Create realistic soil and vegetation parameters
            attributes.append(
                {
                    "divide_id": row["divide_id"],  # StringType
                    "mode.bexp_soil_layers_stag.1": 7.457384,  # DoubleType
                    "mode.bexp_soil_layers_stag.2": 7.457384,  # DoubleType
                    "mode.bexp_soil_layers_stag.3": 7.457384,  # DoubleType
                    "mode.bexp_soil_layers_stag.4": 7.457384,  # DoubleType
                    "mode.ISLTYP": 1.0,  # DoubleType
                    "mode.IVGTYP": 7.0,  # DoubleType
                    "geom_mean.dksat_soil_layers_stag.1": 0.000012,  # DoubleType
                    "geom_mean.dksat_soil_layers_stag.2": 0.000012,  # DoubleType
                    "geom_mean.dksat_soil_layers_stag.3": 0.000012,  # DoubleType
                    "geom_mean.dksat_soil_layers_stag.4": 0.000012,  # DoubleType
                    "geom_mean.psisat_soil_layers_stag.1": -0.355872,  # DoubleType
                    "geom_mean.psisat_soil_layers_stag.2": -0.355872,  # DoubleType
                    "geom_mean.psisat_soil_layers_stag.3": -0.355872,  # DoubleType
                    "geom_mean.psisat_soil_layers_stag.4": -0.355872,  # DoubleType
                    "mean.cwpvt": 0.5,  # DoubleType
                    "mean.mfsno": 2.5,  # DoubleType
                    "mean.mp": 0.0,  # DoubleType
                    "mean.refkdt": 3.0,  # DoubleType
                    "mean.slope_1km": 0.1 + (hash(row["divide_id"]) % 50) / 500.0,  # DoubleType
                    "mean.smcmax_soil_layers_stag.1": 0.476,  # DoubleType
                    "mean.smcmax_soil_layers_stag.2": 0.476,  # DoubleType
                    "mean.smcmax_soil_layers_stag.3": 0.476,  # DoubleType
                    "mean.smcmax_soil_layers_stag.4": 0.476,  # DoubleType
                    "mean.smcwlt_soil_layers_stag.1": 0.135,  # DoubleType
                    "mean.smcwlt_soil_layers_stag.2": 0.135,  # DoubleType
                    "mean.smcwlt_soil_layers_stag.3": 0.135,  # DoubleType
                    "mean.smcwlt_soil_layers_stag.4": 0.135,  # DoubleType
                    "mean.vcmx25": 45.0,  # DoubleType
                    "mean.Coeff": 0.5,  # DoubleType
                    "mean.Zmax": 1.0,  # DoubleType
                    "mode.Expon": 3.0,  # DoubleType
                    "X": -100.0 + (hash(row["divide_id"]) % 1000) / 1000.0,  # DoubleType
                    "Y": 40.0 + (hash(row["divide_id"]) % 500) / 500.0,  # DoubleType
                    "mean.impervious": 0.1,  # DoubleType
                    "mean.elevation": 500.0 + hash(row["divide_id"]) % 1000,  # DoubleType
                    "mean.slope": 0.05 + (hash(row["divide_id"]) % 100) / 1000.0,  # DoubleType
                    "circ_mean.aspect": 180.0,  # DoubleType
                    "dist_4.twi": '[{"v":0.6137,"frequency":0.2501},{"v":2.558,"frequency":0.2499}]',  # StringType
                    "vpuid": "hi",  # StringType
                }
            )

        return pd.DataFrame(attributes)

    def _create_flowpath_attributes(self, flowpath_df: pd.DataFrame) -> pd.DataFrame:
        """Create sample flowpath attributes data matching schema"""

        attributes = []
        for _, row in flowpath_df.iterrows():
            attributes.append(
                {
                    "link": row["id"],  # StringType
                    "to": row["toid"],  # StringType
                    "Length_m": (row["lengthkm"] * 1000) if row["lengthkm"] else None,  # DoubleType
                    "Y": round(0.5 + (hash(row["id"]) % 100) / 200.0, 6),  # DoubleType
                    "n": 0.035,  # DoubleType - Manning's n
                    "nCC": 0.035,  # DoubleType
                    "BtmWdth": 2.0,  # DoubleType
                    "TopWdth": 10.0,  # DoubleType
                    "TopWdthCC": 10.0,  # DoubleType
                    "ChSlp": 0.1,  # DoubleType
                    "alt": 1,  # IntegerType
                    "So": 0.01 + (hash(row["id"]) % 50) / 1000.0,  # DoubleType - slope
                    "MusX": 1800.0,  # DoubleType
                    "MusK": 0.2,  # DoubleType
                    "gage": None,  # StringType
                    "gage_nex_id": None,  # StringType
                    "WaterbodyID": None,  # StringType
                    "waterbody_nex_id": None,  # StringType
                    "id": row["id"],  # StringType
                    "toid": row["toid"],  # StringType
                    "vpuid": "hi",  # StringType
                }
            )

        return pd.DataFrame(attributes)

    def _create_flowpath_attributes_ml(self) -> pd.DataFrame:
        """Create sample ML flowpath attributes data"""
        # This table doesn't exist in the Hawaii schema based on the samples
        # Return empty DataFrame with expected structure
        return pd.DataFrame(columns=["id", "vpuid", "predicted_flow", "confidence"])

    def _create_pois_data(self, network_df: pd.DataFrame) -> pd.DataFrame:
        """Create sample points of interest data matching Hawaii schema"""
        pois = []

        # Create POIs for records that have poi_id
        poi_records = network_df[network_df["poi_id"].notna()]

        for _, row in poi_records.iterrows():
            # Create corresponding nexus ID
            if row["toid"] and row["toid"].startswith("nex-"):
                nex_id = row["toid"]
            else:
                nex_id = f"tnx-{hash(row['id']) % 1000000}"

            pois.append(
                {
                    "poi_id": int(row["poi_id"]),  # IntegerType
                    "id": row["id"],  # StringType
                    "nex_id": nex_id,  # StringType
                    "vpuid": "hi",  # StringType
                }
            )

        return pd.DataFrame(pois)

    def _create_hydrolocations_data(self) -> pd.DataFrame:
        """Create sample hydrolocations data matching Hawaii schema"""
        hydrolocations = []

        # Create some realistic hydrolocations (gages and coastal points)
        sample_hydrolocations = [
            {
                "poi_id": 47,
                "id": "wb-1385",
                "nex_id": "tnx-1000001264",
                "hl_link": "HI2",
                "hl_reference": "coastal",
                "hl_source": "NOAAOWP",
                "hl_uri": "coastal-HI2",
            },
            {
                "poi_id": 38,
                "id": "wb-1384",
                "nex_id": "tnx-1000001226",
                "hl_link": "HI50",
                "hl_reference": "coastal",
                "hl_source": "NOAAOWP",
                "hl_uri": "coastal-HI50",
            },
            {
                "poi_id": 90,
                "id": "wb-1370",
                "nex_id": "nex-1371",
                "hl_link": "16717000",
                "hl_reference": "Gages",
                "hl_source": "NWIS",
                "hl_uri": "Gages-16717000",
            },
            {
                "poi_id": 26,
                "id": "wb-1365",
                "nex_id": "nex-1366",
                "hl_link": "16704000",
                "hl_reference": "Gages",
                "hl_source": "NWIS",
                "hl_uri": "Gages-16704000",
            },
            {
                "poi_id": 12,
                "id": "wb-1366",
                "nex_id": "tnx-1000001261",
                "hl_link": "HI19",
                "hl_reference": "coastal",
                "hl_source": "NOAAOWP",
                "hl_uri": "coastal-HI19",
            },
        ]

        for hl in sample_hydrolocations:
            hydrolocations.append(
                {
                    "poi_id": hl["poi_id"],  # IntegerType
                    "id": hl["id"],  # StringType
                    "nex_id": hl["nex_id"],  # StringType
                    "hl_link": hl["hl_link"],  # StringType
                    "hl_reference": hl["hl_reference"],  # StringType
                    "hl_source": hl["hl_source"],  # StringType
                    "hf_id": 8.000010e13,  # DoubleType
                    "hl_uri": hl["hl_uri"],  # StringType
                    "vpuid": "hi",  # StringType
                }
            )

        return pd.DataFrame(hydrolocations)


# Fixtures for testing
@pytest.fixture
def mock_catalog():
    """Fixture providing a mock Glue catalog"""
    return MockCatalog


@pytest.fixture
def sample_upstream_connections():
    """Fixture providing sample upstream connections data"""
    return SAMPLE_UPSTREAM_CONNECTIONS["upstream_connections"]


@pytest.fixture
def temp_upstream_connections_file(tmp_path):
    """Fixture creating a temporary upstream connections JSON file"""
    connections_file = tmp_path / "hi_upstream_connections.json"
    connections_file.parent.mkdir(parents=True, exist_ok=True)

    with open(connections_file, "w") as f:
        json.dump(SAMPLE_UPSTREAM_CONNECTIONS, f)

    return connections_file


# Utility functions for test setup
def create_test_environment(tmp_path: Path) -> dict[str, Any]:
    """Create a complete test environment with mock catalogs and data files"""

    # Create upstream connections file for Hawaii
    connections_file = tmp_path / "data" / "hydrofabric" / "hi_upstream_connections.json"
    connections_file.parent.mkdir(parents=True, exist_ok=True)

    with open(connections_file, "w") as f:
        json.dump(SAMPLE_UPSTREAM_CONNECTIONS, f)

    # Create mock catalogs
    glue_catalog = MockCatalog("glue")
    sql_catalog = MockCatalog("sql")

    return {
        "glue_catalog": glue_catalog,
        "sql_catalog": sql_catalog,
        "connections_file": connections_file,
        "tmp_path": tmp_path,
    }


## Uncomment to test these conftest mock objects
if __name__ == "__main__":
    # Example usage and verification
    print("Creating hydrofabric mock catalog...")
    catalog = MockCatalog()

    # Test loading all tables
    tables_to_test = [
        "mock_hf.network",
        "mock_hf.flowpaths",
        "mock_hf.nexus",
        "mock_hf.divides",
        "mock_hf.lakes",
        "mock_hf.divide-attributes",
        "mock_hf.flowpath-attributes",
        "mock_hf.pois",
        "mock_hf.hydrolocations",
    ]

#     print("\nTable verification:")
#     for table_name in tables_to_test:
#         table = catalog.load_table(table_name)
#         df = table.to_polars()
#         print(f"✓ {table_name}: {df.height} records, {len(df.columns)} columns")

#         # Show first few columns for verification
#         cols_to_show = df.columns[:5]
#         print(f"  Columns: {cols_to_show}")

#     # Test network table structure specifically
#     print("\nNetwork table detailed verification:")
#     network_table = catalog.load_table("mock_hf.network")
#     network_df = network_table.to_polars()

#     print(f"Total records: {network_df.height}")
#     wb_records = network_df.filter(pl.col("topo") == "fl-wb")
#     nex_records = network_df.filter(pl.col("topo") == "fl-nex")
#     print(f"Flowpath records (fl-wb): {wb_records.height}")
#     print(f"Nexus records (fl-nex): {nex_records.height}")

#     # Check VPU
#     vpu_values = set(network_df.get_column("vpuid").to_list())
#     print(f"VPU values: {vpu_values}")

#     # Check for POIs
#     poi_records = network_df.filter(pl.col("poi_id").is_not_null())
#     print(f"Records with POI IDs: {poi_records.height}")

#     # Test filtering
#     print("\nTesting table filtering:")
#     vpu_filter = EqualTo("vpuid", "hi")
#     filtered_scan = network_table.scan(row_filter=vpu_filter)
#     filtered_data = filtered_scan.to_polars()
#     print(f"Filtered by VPU 'hi': {filtered_data.height} records")

#     # Test upstream connections consistency
#     print("\nTesting upstream connections:")
#     sample_connections = SAMPLE_UPSTREAM_CONNECTIONS["upstream_connections"]
#     found_connections = 0

#     for parent, children in list(sample_connections.items())[:5]:
#         parent_records = network_df.filter(pl.col("id") == parent)
#         if parent_records.height > 0:
#             print(f"✓ Found parent: {parent}")
#             for child in children:
#                 child_records = network_df.filter(pl.col("id") == child)
#                 if child_records.height > 0:
#                     child_toid = child_records.get_column("toid")[0]
#                     if child_toid == parent:
#                         print(f"  ✓ Child {child} correctly points to {parent}")
#                         found_connections += 1
#                     else:
#                         print(f"  ✗ Child {child} points to {child_toid}, not {parent}")

#     print(f"Verified {found_connections} upstream connections")

#     # Test hydrolocations
#     print("\nTesting hydrolocations:")
#     hydrolocations_table = catalog.load_table("mock_hf.hydrolocations")
#     hydrolocations_df = hydrolocations_table.to_polars()
#     print(f"Hydrolocations: {hydrolocations_df.height} records")

#     if hydrolocations_df.height > 0:
#         print("Sample hl_uri values:")
#         for hl_uri in hydrolocations_df.get_column("hl_uri").to_list()[:3]:
#             print(f"  - {hl_uri}")

#     print("\nMock catalog verification completed successfully!")
#     print("You can now use this catalog to test your hydrofabric functions.")
#     print("\nExample usage:")
#     print("  catalog = MockCatalog()")
#     print("  network_table = catalog.load_table('mock_hf.network')")
#     print("  network_df = network_table.to_polars()")
#     print("  # Test find_origin with: 'Gages-16717000' or 'coastal-HI2'")
#     print("  # Test upstream tracing with: 'wb-2700' (has multiple upstream)")
