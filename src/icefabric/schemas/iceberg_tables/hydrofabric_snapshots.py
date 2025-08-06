"""Contains the PyIceberg Table schema for all hydrofabric layers"""

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField


class HydrofabricSnapshot:
    """The schema containing all snapshots of the layers for the Hydrofabric. This is used to version control many layers

    Attributes
    ----------
    - divide-attributes
    - divides
    - flowpath-attributes
    - flowpath-attributes-ml
    - flowpaths
    - hydrolocations
    - lakes
    - network
    - nexus
    - pois
    """

    @classmethod
    def columns(cls) -> list[str]:
        """Returns the columns associated with this schema

        Returns
        -------
        list[str]
            The schema columns
        """
        return [
            "divide-attributes",
            "divides",
            "flowpath-attributes",
            "flowpath-attributes-ml",
            "flowpaths",
            "hydrolocations",
            "lakes",
            "network",
            "nexus",
            "pois",
        ]

    @classmethod
    def schema(cls) -> Schema:
        """Returns the PyIceberg Schema object.

        Returns
        -------
        Schema
            PyIceberg schema for Hydrofabric
        """
        return Schema(
            NestedField(1, "divide-attributes", IntegerType(), required=False),
            NestedField(2, "divides", IntegerType(), required=False),
            NestedField(3, "flowpath-attributes", IntegerType(), required=False),
            NestedField(4, "flowpath-attributes-ml", IntegerType(), required=False),
            NestedField(5, "flowpaths", IntegerType(), required=False),
            NestedField(6, "hydrolocations", IntegerType(), required=False),
            NestedField(7, "lakes", IntegerType(), required=False),
            NestedField(8, "network", IntegerType(), required=False),
            NestedField(9, "nexus", IntegerType(), required=False),
            NestedField(10, "pois", IntegerType(), required=False),
        )

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        """Returns the PyArrow Schema object.

        Returns
        -------
        pa.Schema
            PyArrow schema for Hydrofabric
        """
        return pa.schema(
            [
                pa.field("divide-attributes", pa.int32(), nullable=True),
                pa.field("divides", pa.int32(), nullable=True),
                pa.field("flowpath-attributes", pa.int32(), nullable=True),
                pa.field("flowpath-attributes-ml", pa.int32(), nullable=True),
                pa.field("flowpaths", pa.int32(), nullable=True),
                pa.field("hydrolocations", pa.int32(), nullable=True),
                pa.field("lakes", pa.int32(), nullable=True),
                pa.field("network", pa.int32(), nullable=True),
                pa.field("nexus", pa.int32(), nullable=True),
                pa.field("pois", pa.int32(), nullable=True),
            ]
        )
