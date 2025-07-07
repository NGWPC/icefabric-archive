import httpx
import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd


def get_observational_uri(
    gage_id: str, source: str = "USGS", domain: str = "CONUS", version: str = "2.1", timeout=None
) -> str:
    """Fetch observational data URI from the NextGen Water Prediction API.

    Retrieves the URI for observational streamflow data for a specific gage
    from the NextGen Water Prediction hydrofabric API.

    Parameters
    ----------
    gage_id : str
        The gage identifier (e.g., USGS station ID).
    source : str, default "USGS"
        Data source provider for the observational data.
    domain : str, default "CONUS"
        Geographic domain name for the data request.
    version : str, default "2.1"
        API version to use for the request.
    timeout : float or None, optional
        Request timeout in seconds. If None, uses httpx default.

    Returns
    -------
    str
        The URI pointing to the observational dataset.
    """
    base_url = f"https://hydroapi.oe.nextgenwaterprediction.com/hydrofabric/{version}/observational"
    params = {"gage_id": gage_id, "source": source, "domain": domain}

    response = httpx.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()  # Raise an error if request failed
    data = response.json()

    return data["uri"]


def get_geopackage_uri(
    gage_id: str, source: str = "USGS", domain: str = "CONUS", version: str = "2.2", timeout=None
) -> str:
    """Fetch GeoPackage URI for a gage from the NextGen Water Prediction API.

    Retrieves the URI for a hydrofabric GeoPackage containing network topology
    and catchment boundaries for a specific gage from the NextGen API.

    Parameters
    ----------
    gage_id : str
        The gage identifier for which to retrieve the hydrofabric GeoPackage.
    source : str, default "USGS"
        Data source provider for the gage data.
    domain : str, default "CONUS"
        Geographic domain name for the hydrofabric request.
    version : str, default "2.2"
        Hydrofabric version to retrieve.
    timeout : float or None, optional
        Request timeout in seconds. If None, uses httpx default.

    Returns
    -------
    str
        The URI pointing to the GeoPackage file in S3 storage.
    """
    base_url = "https://hydroapi.oe.nextgenwaterprediction.com/hydrofabric/geopackages"
    params = {"gage_id": gage_id, "source": source, "domain": domain, "version": version}

    response = httpx.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    return data["uri"]


def create_time_series_widget(
    df: pd.DataFrame,
    start_slider: widgets.SelectionSlider,
    end_slider: widgets.SelectionSlider,
    point_size: float = 30,
):
    """
    Creates an interactive time series plot using matplotlib and ipywidgets.

    Parameters
    ----------
        df (pd.DataFrame): DataFrame with 'DateTime' and 'q_cms' columns
        start_slider (widgets.SelectionSlider): Widget for selecting start time
        end_slider (widgets.SelectionSlider): Widget for selecting end time
    """

    @widgets.interact(start=start_slider, end=end_slider)
    def plot_flow(start, end):
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)

        if start_dt >= end_dt:
            print("Warning: Start must be before End.")
            return

        filtered = df[(df["dateTime"] >= start_dt) & (df["dateTime"] <= end_dt)]

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.scatter(filtered["dateTime"], filtered["q_cms"], s=point_size, label="Flow rate (cms)", alpha=0.7)
        ax.set_xlabel("Date Time")
        ax.set_ylabel("Discharge (cms)")
        ax.set_title("Streamflow Time Series (Scatter Plot)")
        ax.grid(True)
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
