import os

import geopandas as gpd
import pandas as pd
import pyiceberg.exceptions as ex
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyproj import Transformer

ROOT_DIR = os.path.abspath(os.curdir)


def module_ipe(module: str, attr: str, domain: str, version: str, cfg_write=None):
    """Creates initial parameter estimates for a module

    Parameters
    ----------
    module : str
        _description_
    attr : str
        _description_
    domain : str
        _description_
    version : str
        _description_
    cfg_write : str, optional
        _description_, by default None

    Returns
    -------
    _type_
        _description_
    """
    # Create empty dataframe for csv parameters
    csv_params = pd.DataFrame()

    # Get list of divides
    divides = attr["divide_id"].to_list()

    # Look up parameter csv file for module
    has_output_vars = True
    module_df = pd.read_csv(f"{ROOT_DIR}/data/modules.csv")
    param_file = module_df.loc[module_df["module"] == module]["file"].to_string(index=False)
    out_file = module_df.loc[module_df["module"] == module]["outputs"].to_string(index=False)
    if out_file == "NaN":
        has_output_vars = False

    # Get module parameters
    datatypes = {
        "name": "object",
        "description": "object",
        "units": "object",
        "data_type": "object",
        "calibratable": "bool",
        "source": "object",
        "min": "float64",
        "max": "float64",
        "default_value": "object",
        "divide_attr_name": "object",
        "source_file": "object",
    }
    # Get output vars
    out_datatypes = {"variable": "object", "description": "object"}

    module_params = pd.read_csv(f"{ROOT_DIR}/data/{param_file}", dtype=datatypes)
    output_vars = pd.DataFrame()
    if has_output_vars:
        output_vars = pd.read_csv(f"{ROOT_DIR}/data/{out_file}", dtype=out_datatypes)

    # Filter data frame for parameter names and default values.  Create dictionary to collect
    # parameters for cfg files.
    param_values = module_params[["name", "default_value"]]
    param_values = pd.Series(param_values["default_value"].values, index=param_values["name"]).to_dict()

    # Get sources
    attr_list = module_params["source"].to_list()

    # Create dictionary with divide IDs as the keys and parameter dictionary for the values.
    all_cats = {}
    for divide in divides:
        all_cats[divide] = param_values.copy()

    # For attributes, get list of attribute names
    attr_list = module_params.loc[module_params["source"] == "attr"]["divide_attr_name"].to_list()
    attr_list.append("divide_id")
    # Get corresponding list of parameter names to create a dictionary for mapping
    attr_names = module_params.loc[module_params["source"] == "attr"]["name"].to_list()
    attr_name_map = dict(zip(attr_list, attr_names, strict=False))
    # Filter attributes for those needed for this module
    attr = attr[attr_list]
    # Rename attribute names to module names
    attr.rename(columns=attr_name_map, inplace=True)

    # Get iceberg table parameters
    if "iceberg" in module_params["source"].values:
        iceberg_params = divide_parameters(divides, module, domain)

    # Loop through divides and update default values with divide attributes from geopackage
    # and iceberg tables
    for divide in divides:
        attr_div = attr.loc[attr["divide_id"] == divide]
        attr_div = attr_div.to_dict(orient="list")
        all_cats[divide].update(attr_div)
        if not iceberg_params.empty:
            iceberg_params_div = iceberg_params.loc[iceberg_params["divide_id"] == divide]
            iceberg_params_div = iceberg_params_div.to_dict(orient="list")
            all_cats[divide].update(iceberg_params_div)

        # When dataframe is converted to a dictionary, the values are contained in a list
        # Remove the value from the list.
        for key in all_cats[divide].keys():
            value = all_cats[divide][key]
            if isinstance(value, list):
                all_cats[divide][key] = value[0]

    if cfg_write:
        _write_cfg(all_cats)
    s3_uri = "s3://"
    module_json = _write_json(all_cats[divides[1]], module_params, output_vars, module, s3_uri)
    return module_json


def _write_cfg(params: dict):
    """Writes a config file from a dictionary of parameters"""
    divides = params.keys()
    for divide in divides:
        cfg_file = f"{ROOT_DIR}/{divide}_bmi_config_cfe.txt"
        f = open(cfg_file, "w")

        for key, value in params[divide].items():
            f.write(f"{key}={value}\n")

    f.close()


def _write_json(params, all_params, output_vars, module, s3_uri):
    """Writes a JSON file from a dictionary of parameters"""
    json_columns = ["name", "description", "default_value", "min", "max", "data_type", "units"]
    all_params = all_params.loc[all_params["calibratable"] == True, json_columns]
    all_params = all_params.to_dict(orient="records")

    for item in all_params:
        name = item["name"]
        value = params[name]
        item["default_value"] = value

    module_dict = {
        "module_name": module,
        "parameter_file": {"uri": s3_uri},
        "calibrate_parameters": all_params,
    }
    if not output_vars.empty:
        output_vars = output_vars.to_dict(orient="records")
        module_dict.update({"output_variables": output_vars})

    return module_dict


def get_hydrofabric_attributes(gpkg_file, version, domain):
    """Gets attributes from the hydrofabric geopackage"""
    attr_layer = "divide-attributes"
    if version == "2.1":
        attr_layer = "model-attributes"

    # Map oCONUS lat/lon attribute names to CONUS names.
    column_names_xy = {"X": "centroid_x", "Y": "centroid_y"}
    # Puerto Rico has a number of attribute names that don't match the other domains in HF v2.2
    column_names_pr = {
        "dksat_Time._soil_layers_stag.1": "geom_mean.dksat_soil_layers_stag.1",
        "dksat_Time._soil_layers_stag.2": "geom_mean.dksat_soil_layers_stag.2",
        "dksat_Time._soil_layers_stag.3": "geom_mean.dksat_soil_layers_stag.3",
        "dksat_Time._soil_layers_stag.4": "geom_mean.dksat_soil_layers_stag.4",
        "mean.cwpvt_Time.": "mean.cwpvt",
        "mean.mfsno_Time.": "mean.mfsno",
        "mean.mp_Time.": "mean.mp",
        "mean.refkdt_Time.": "mean.refkdt",
        "mean.slope_Time.": "mean.slope_1km",
        "mean.smcmax_Time._soil_layers_stag.1": "mean.smcmax_soil_layers_stag.1",
        "mean.smcmax_Time._soil_layers_stag.2": "mean.smcmax_soil_layers_stag.2",
        "mean.smcmax_Time._soil_layers_stag.3": "mean.smcmax_soil_layers_stag.3",
        "mean.smcmax_Time._soil_layers_stag.4": "mean.smcmax_soil_layers_stag.4",
        "mean.smcwlt_Time._soil_layers_stag.1": "mean.smcwlt_soil_layers_stag.1",
        "mean.smcwlt_Time._soil_layers_stag.2": "mean.smcwlt_soil_layers_stag.2",
        "mean.smcwlt_Time._soil_layers_stag.3": "mean.smcwlt_soil_layers_stag.3",
        "mean.smcwlt_Time._soil_layers_stag.4": "mean.smcwlt_soil_layers_stag.4",
        "mean.vcmx25_Time.": "mean.vcmx25",
        "mode.bexp_Time._soil_layers_stag.1": "mode.bexp_soil_layers_stag.1",
        "mode.bexp_Time._soil_layers_stag.2": "mode.bexp_soil_layers_stag.2",
        "mode.bexp_Time._soil_layers_stag.3": "mode.bexp_soil_layers_stag.3",
        "mode.bexp_Time._soil_layers_stag.4": "mode.bexp_soil_layers_stag.4",
        "psisat_Time._soil_layers_stag.1": "geom_mean.psisat_soil_layers_stag.1",
        "psisat_Time._soil_layers_stag.2": "geom_mean.psisat_soil_layers_stag.2",
        "psisat_Time._soil_layers_stag.3": "geom_mean.psisat_soil_layers_stag.3",
        "psisat_Time._soil_layers_stag.4": "geom_mean.psisat_soil_layers_stag.4",
    }

    # Get list of catchments from gpkg divides layer using geopandas
    try:
        divide_attr = gpd.read_file(gpkg_file, layer=attr_layer)
        divide_layer = gpd.read_file(gpkg_file, layer="divides")
    except:  # TODO: Replace 'except' with proper catch
        error_str = "Error opening " + gpkg_file
        error = dict(error=error_str)
        print(error_str)
        return error

    # Get catchement area from divides layer and append to attributes data frame
    area = divide_layer[["divide_id", "areasqkm"]]
    divide_attr = divide_attr.join(area.set_index("divide_id"), on="divide_id")

    # Account for differences in column names between CONUS and oCONUS
    if version == "2.2" and domain != "CONUS":
        divide_attr.rename(columns=column_names_xy, inplace=True)
    if version == "2.2" and domain == "Puerto_Rico":
        divide_attr.rename(columns=column_names_pr, inplace=True)

    # Soil and vegetation types are read from the gpkg as floats, but need to be ints
    if version == "2.1":
        divide_attr = divide_attr.astype({"ISLTYP": "int"})
        divide_attr = divide_attr.astype({"IVGTYP": "int"})
    elif version == "2.2":
        divide_attr = divide_attr.astype({"mode.ISLTYP": "int"})
        divide_attr = divide_attr.astype({"mode.IVGTYP": "int"})

    # Zmax/max_gw_storage units are mm in the hydrofabric but CFE expects m.
    if version == "2.1":
        divide_attr["gw_Zmax"] = divide_attr["gw_Zmax"].apply(lambda x: x / 1000)

    elif version == "2.2":
        divide_attr["mean.Zmax"] = divide_attr["mean.Zmax"].apply(lambda x: x / 1000)

    # Elevation in 2.2 is in cm, convert to m.  Except for AK, which is still in m.
    if version == "2.2" and domain != "Alaska":
        divide_attr["mean.elevation"] = divide_attr["mean.elevation"].apply(lambda x: x / 100)

    # Convert centroid_x and centroid_y (lat/lon) from the domain's CRS to WGS84
    # for decimal degrees for 2.2.
    if version == "2.2":
        crs = divide_layer.crs
        transformer = Transformer.from_crs(crs, 4326)
        for index, row in divide_attr.iterrows():
            y = row["centroid_y"]
            x = row["centroid_x"]
            wgs84_latlon = transformer.transform(x, y)
            divide_attr.loc[index, "centroid_y"] = wgs84_latlon[0]  # latitude
            divide_attr.loc[index, "centroid_x"] = wgs84_latlon[1]  # longitude

    return divide_attr


def divide_parameters(divides, module, domain):
    """Returns iceberg divide parameters"""
    module = module.lower()
    domain = domain.lower()
    namespace = "divide_parameters"
    table_name = f"{namespace}.{module}_{domain}"
    catalog = load_catalog("glue")
    try:
        table = catalog.load_table(table_name)
    except ex.NoSuchTableError:
        print(f"Table {table_name} does not exist")
        return pd.DataFrame()
    except ClientError as e:
        msg = "AWS Test account credentials expired. Can't access remote S3 endpoint"
        print(msg)
        raise e
    df = table.scan().to_pandas()
    filtered = df[df["divide_id"].isin(divides)]
    return filtered
