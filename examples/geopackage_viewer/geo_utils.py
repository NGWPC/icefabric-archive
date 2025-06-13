import pandas as pd
import httpx
import folium
import matplotlib.pyplot as plt
import ipywidgets as widgets
from ipywidgets import interact

def get_observational_uri(gage_id: str, source: str = "USGS", domain: str = "CONUS", version: str = "2.1", timeout = None) -> str:
    """
    Fetches the observational data URI from the NextGen Water Prediction API.

    Args:
        gage_id (str): The gage identifier.
        source (str): Data source, e.g., "USGS".
        domain (str): Domain name, e.g., "CONUS".
        version (str): API version, e.g., "2.2".

    Returns:
        str: The URI of the observational dataset.
    """
    base_url = f"https://hydroapi.oe.nextgenwaterprediction.com/hydrofabric/{version}/observational"
    params = {
        "gage_id": gage_id,
        "source": source,
        "domain": domain
    }

    response = httpx.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()  # Raise an error if request failed
    data = response.json()

    return data["uri"]
    

def get_geopackage_uri(gage_id: str, source: str = "USGS", domain: str = "CONUS", version: str = "2.2", timeout=None) -> str:
    """
    Fetches the GeoPackage URI for a given gage ID from the NextGen Water Prediction API.

    Returns:
        str: The URI pointing to the GeoPackage in S3.
    """
    import httpx

    base_url = "https://hydroapi.oe.nextgenwaterprediction.com/hydrofabric/geopackages"
    params = {
        "gage_id": gage_id,
        "source": source,
        "domain": domain,
        "version": version
    }

    response = httpx.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    return data["uri"]


def create_time_series_widget(df: pd.DataFrame, start_slider: widgets.SelectionSlider, end_slider: widgets.SelectionSlider, point_size: float = 30):
    """
    Creates an interactive time series plot using matplotlib and ipywidgets.
    
    Parameters:
        df (pd.DataFrame): DataFrame with 'DateTime' and 'q_cms' columns
        start_slider (widgets.SelectionSlider): Widget for selecting start time
        end_slider (widgets.SelectionSlider): Widget for selecting end time
    """
    @widgets.interact(start=start_slider, end=end_slider)
    def plot_flow(start, end):
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)

        if start_dt >= end_dt:
            print("⚠️ Start must be before End.")
            return

        filtered = df[(df['dateTime'] >= start_dt) & (df['dateTime'] <= end_dt)]

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.scatter(filtered['dateTime'], filtered['q_cms'], s=point_size, label="Flow rate (cms)", alpha=0.7)
        ax.set_xlabel("Date Time")
        ax.set_ylabel("Discharge (cms)")
        ax.set_title("Streamflow Time Series (Scatter Plot)")
        ax.grid(True)
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()