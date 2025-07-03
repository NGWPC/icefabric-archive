"""Contains all schemas and enums for the NGWPC Enterprise Hydrofabric"""

from enum import Enum


class IdType(str, Enum):
    """All queriable HF fields.

    Attributes
    ----------
    HL_URI : str
        Hydrolocation URI identifier
    HF_ID : str
        Hydrofabric ID identifier
    ID : str
        Generic ID identifier
    POI_ID : str
        Point of Interest ID identifier
    """

    HL_URI = "hl_uri"
    HF_ID = "hf_id"
    ID = "id"
    POI_ID = "poi_id"


# For catchments that may extend in many VPUs
UPSTREAM_VPUS: dict[str, list[str]] = {"08": ["11", "10U", "10L", "08", "07", "05"]}
