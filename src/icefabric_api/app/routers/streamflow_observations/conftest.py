import pandas as pd
import pytest


@pytest.fixture
def local_usgs_streamflow_csv():
    """Returns a locally downloaded CSV file from a specific gauge and time"""
    return pd.read_csv("../../tests/data/usgs_01010000_data_from_20211231_1400_to_20220101_1400.csv")


@pytest.fixture
def local_usgs_streamflow_parquet():
    """Returns a locally downloaded CSV file from a specific gauge and time"""
    return pd.read_parquet("../../tests/data/usgs_01010000_data_from_20211231_1400_to_20220101_1400.parquet")
