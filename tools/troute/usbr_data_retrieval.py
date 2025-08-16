"""A simple script to convert the v2.2 hydrofabric to parquet"""

import argparse
from pathlib import Path

from icefabric.helpers import EXT_RISE_BASE_URL, load_creds, make_get_req_to_rise

load_creds(dir=Path.cwd())

RISE_HEADERS = {"accept": "application/vnd.api+json"}

"""
RFC
---
>>> xr.open_dataset("/Users/taddbindas/projects/NGWPC/icefabric/data/2023-04-01_00.60min.BCRT2.RFCTimeSeries.ncdf")
<python-input-1>:1: FutureWarning: In a future version, xarray will not decode timedelta values based on the presence of a timedelta-like units attribute by default. Instead it will rely on the presence of a timedelta64 dtype attribute, which is now xarray's default way of encoding timedelta64 values. To continue decoding timedeltas based on the presence of a timedelta-like units attribute, users will need to explicitly opt-in by passing True or CFTimedeltaCoder(decode_via_units=True) to decode_timedelta. To silence this warning, set decode_timedelta to True, False, or a 'CFTimedeltaCoder' instance.
  xr.open_dataset("/Users/taddbindas/projects/NGWPC/icefabric/data/2023-04-01_00.60min.BCRT2.RFCTimeSeries.ncdf")
<xarray.Dataset> Size: 1kB
Dimensions:              (nseries: 1, forecastInd: 289)
Dimensions without coordinates: nseries, forecastInd
Data variables:
    stationId            |S5 5B ...
    issueTimeUTC         (nseries) |S19 19B ...
    discharges           (nseries, forecastInd) float32 1kB ...
    synthetic_values     (nseries, forecastInd) int8 289B ...
    totalCounts          (nseries) int16 2B ...
    observedCounts       (nseries) int16 2B ...
    forecastCounts       (nseries) int16 2B ...
    timeSteps            (nseries) timedelta64[ns] 8B ...
    discharge_qualities  (nseries) int16 2B ...
    queryTime            (nseries) datetime64[ns] 8B ...
Attributes:
    fileUpdateTimeUTC:           2023-04-03_00:42:00
    sliceStartTimeUTC:           2023-03-30_00:00:00
    sliceTimeResolutionMinutes:  60
    missingValue:                -999.99
    newest_forecast:             0
    NWM_version_number:          v2.1

USGS:
----
>>> xr.open_dataset("/Users/taddbindas/projects/NGWPC/icefabric/data/2023-04-01_00_00_00.15min.usgsTimeSlice.ncdf")
<xarray.Dataset> Size: 3kB
Dimensions:            (stationIdInd: 57)
Dimensions without coordinates: stationIdInd
Data variables:
    stationId          (stationIdInd) |S15 855B ...
    time               (stationIdInd) |S19 1kB ...
    discharge          (stationIdInd) float32 228B ...
    discharge_quality  (stationIdInd) int16 114B ...
    queryTime          (stationIdInd) datetime64[ns] 456B ...
Attributes:
    fileUpdateTimeUTC:           2023-04-01_04:54:15
    sliceCenterTimeUTC:          2023-04-01_00:00:00
    sliceTimeResolutionMinutes:  15

USACE
-----
>>> xr.open_dataset("/Users/taddbindas/projects/NGWPC/icefabric/data/2023-04-01_00_00_00.15min.usaceTimeSlice.ncdf", decode_times=False)
<xarray.Dataset> Size: 0B
Dimensions:            (stationIdInd: 0)
Dimensions without coordinates: stationIdInd
Data variables:
    stationId          (stationIdInd) |S15 0B ...
    time               (stationIdInd) |S19 0B ...
    discharge          (stationIdInd) float32 0B ...
    discharge_quality  (stationIdInd) int16 0B ...
    queryTime          (stationIdInd) int32 0B ...
Attributes:
    fileUpdateTimeUTC:           2023-04-01_04:55:00
    sliceCenterTimeUTC:          2023-04-01_00:00:00
    sliceTimeResolutionMinutes:  15
"""


async def get_location_by_id(id: str):
    """Retrieves a Location resource, per a given ID."""
    rise_response = await make_get_req_to_rise(f"{EXT_RISE_BASE_URL}/location/{id}")
    return rise_response["detail"]


def result_to_file(location_id: str, output_folder: Path) -> None:
    """Calls the USBR API and formats the response for t-route

    Parameters
    ----------
    location_id : str
        the usbr reservoir ID
    output_folder : Path
        The path to the folder to dump the outputs
    """
    raise NotImplementedError


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert USBR reservoir outflows into data to be used in RFC-DA"
    )

    parser.add_argument(
        "--location-id", type=str, required=True, help="The location ID to get pull streamflow from"
    )
    parser.add_argument(
        "--output-folder",
        type=Path,
        default=Path.cwd(),
        help="Output directory for parquet file (default is cwd)",
    )

    args = parser.parse_args()
    result_to_file(location_id=args.location_id, output_folder=args.output_folder)
