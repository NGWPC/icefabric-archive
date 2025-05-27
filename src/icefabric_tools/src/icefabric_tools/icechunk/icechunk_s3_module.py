"""
NGWPC Icechunk interface module

Module containing classes/methods pertaining
to S3 pathing and Icechunk repos
"""

import subprocess
import warnings
from enum import Enum
from pathlib import Path
from typing import Any

import icechunk as ic
import xarray as xr
from icechunk.xarray import to_icechunk

TOPO_BP = "surface/nws-topobathy"
TOPO_NOS = f"{TOPO_BP}/nws-nos-surveys"


class S3Path:
    """
    Class representing an S3 path.

    Corresponds to an S3 bucket, prefix and region

    Parameters
    ----------
    bucket: str
        The bucket of the S3 path.
    prefix: str
        The S3 path (minus the bucket).
    region: str
        The S3 region the bucket/path belongs to. Defaults to 'us-east-1'.
    """

    bucket: str
    prefix: str
    region: str

    def __init__(self, bucket: str, prefix: str, region: str | None = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix
        self.region = region

    def __str__(self):
        """Returns the full S3 path"""
        return f"s3://{self.bucket}/{self.prefix}"

    def partial_path(self):
        """Returns the S3 path without the 'S3://' prefix"""
        return f"{self.bucket}/{self.prefix}"


class NGWPCLocations(Enum):
    """
    Important NGWPC S3 locations

    Enum class for instantiating S3Paths corresponding to the
    icechunk stores, as well as the reference locations for virtualized
    stores.
    """

    SNODAS_REF = ("ngwpc-forcing", "snodas_nc_v4")
    SNODAS_V3 = ("ngwpc-forcing", "snodas_nc")
    SNODAS_IC = ("hydrofabric-data", "forcing/snodas")
    NLCD_REF = ("ngwpc-hydrofabric", "NLCD_Land_Cover_CONUS")
    NLCD_IC = ("hydrofabric-data", "land-cover/NLCD-Land-Cover")
    USGS_IC = ("ngwpc-hydrofabric", "usgs_observations")
    TOPO_AK_10M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_alaska_10m")
    TOPO_AK_30M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_alaska_30m")
    TOPO_CONUS_ATL_GULF_30M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_conus_atlantic_gulf_30m")
    TOPO_CONUS_PAC_30M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_conus_pacific_30m")
    TOPO_GREAT_LAKES_30M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_great_lakes_30m")
    TOPO_HA_10M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_hawaii_10m")
    TOPO_HA_30M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_hawaii_30m")
    TOPO_PR_USVI_10M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_pr_usvi_10m")
    TOPO_PR_USVI_30M_IC = ("hydrofabric-data", f"{TOPO_BP}/tbdem_pr_usvi_30m")
    TOPO_ALBEMARLE_SOUND_IC = ("hydrofabric-data", f"{TOPO_NOS}/Albemarle_Sound_NOS_NCEI")
    TOPO_CHESAPEAKE_BAY_IC = ("hydrofabric-data", f"{TOPO_NOS}/Chesapeake_Bay_NOS_NCEI")
    TOPO_MOBILE_BAY_IC = ("hydrofabric-data", f"{TOPO_NOS}/Mobile_Bay_NOS_NCEI")
    TOPO_TANGIER_SOUND_IC = ("hydrofabric-data", f"{TOPO_NOS}/Tangier_Sound_NOS_NCEI")

    def __init__(self, bucket, prefix):
        self.path = S3Path(bucket, prefix)


class IcechunkS3Repo:
    """
    Class representing an S3 bucket icechunk store

    Parameters
    ----------
    location: S3Path
        The S3Path of the repo.
    repo: ic.Repository
        The icechunk repo, derived from the bucket, prefix, and region. S3
        credentials are provided from the environment.
    virtual_chunks: list[ic.VirtualChunkContainer] | None
        A list of virtual chunk containers corresponding to reference data
        for virtualized stores. Allows icechunk to reference S3 locations
        in virtualized datasets.
    """

    location: S3Path
    repo: ic.Repository
    virtual_chunks: list[ic.VirtualChunkContainer] | None

    def __init__(self, location: S3Path, virtual_chunk_mapping: list[dict[str, str]] | None = None):
        self.location = location
        self.virtual_chunks = self.gen_virtual_chunk_containers(virtual_chunk_mapping)
        self.repo = self.open_repo()

    def open_repo(self) -> ic.Repository:
        """
        Opens an icechunk repo

        Using the class instance parameters, open and assign an icechunk repo corresponding
        to the setup (bucket, prefix, region, etc.)

        Returns
        -------
        ic.Repository
            Icechunk repo corresponding to the S3 bucket path defined in the instance
        """
        storage = ic.s3_storage(
            bucket=self.location.bucket,
            prefix=self.location.prefix,
            region=self.location.region,
            from_env=True,
        )
        config = ic.RepositoryConfig.default()
        if self.virtual_chunks:
            for vcc in self.virtual_chunks:
                config.set_virtual_chunk_container(vcc)
        credentials = ic.containers_credentials({self.location.bucket: ic.s3_credentials(from_env=True)})
        repo = ic.Repository.open_or_create(storage, config, credentials)
        return repo

    def delete_repo(self, quiet: bool | None = False):
        """
        Deletes the entire icechunk repo from S3.

        Parameters
        ----------
        quiet : bool | None, optional
            Suppresses AWS CLI output. By default False
        """
        del_command = ["aws", "s3", "rm", str(self.location), "--recursive"]
        if quiet:
            del_command.append("--quiet")
        subprocess.call(del_command)
        print(f"Icechunk repo @ {str(self.location)} in its entirety was successfully deleted.")

    def gen_virtual_chunk_containers(
        self, virtual_chunk_mapping: list[dict[str, str]] | None = None
    ) -> list[ic.VirtualChunkContainer]:
        """
        Create a list of virtual chunk containers

        Given a list of dictionaries mapping out virtual chunks, generate
        and return a list of VirtualChunkContainers

        Parameters
        ----------
        virtual_chunk_mapping : list[dict[str, str]] | None, optional
            A list of dictionaries, each entry mapping out a single
            virtual chunk definition. Should include a bucket and region.
            By default None

        Returns
        -------
        list[ic.VirtualChunkContainer]
            A list of VirtualChunkContainers corresponding to the list of passed-in
            dict mappings.
        """
        v_chunks = None
        if virtual_chunk_mapping:
            v_chunks = [
                set_up_virtual_chunk_container(vc["bucket"], vc["region"]) for vc in virtual_chunk_mapping
            ]
        return v_chunks

    def create_session(self, read_only: bool | None = True, branch: str | None = "main") -> ic.Session:
        """
        Open a session under the repo defined by an instance of IcechunkS3Repo

        Parameters
        ----------
        read_only : bool | None, optional
            Denotes if the session will be read-only or writable. By default True
        branch : str | None, optional
            Icechunk repo branch to be opened. By default "main"

        Returns
        -------
        ic.Session
            Icechunk repo session. Writable or read-only based on parameters. Branch
            can be configured.
        """
        if read_only:
            return self.repo.readonly_session(branch)
        return self.repo.writable_session(branch)

    def retrieve_dataset(self, read_only: bool | None = True, branch: str | None = "main") -> xr.Dataset:
        """
        Returns the repo's store contents as an Xarray dataset

        Parameters
        ----------
        read_only : bool | None, optional
            Denotes if the session will be read-only or writable, by default True
        branch : str | None, optional
            Icechunk repo branch to be opened, by default "main"

        Returns
        -------
        xr.Dataset
            Xarray dataset representation of the Icechunk store
        """
        session = self.create_session(read_only, branch)
        ds = xr.open_zarr(session.store, consolidated=False, chunks={})

        # geotiff rasters saved in zarr need to be convereted to spatial-aware xarray with rioxarray
        if "spatial_ref" in ds.data_vars:
            ds.rio.write_crs(ds.spatial_ref.spatial_ref, inplace=True)

        return ds

    def write_dataset(
        self, ds: xr.Dataset, commit: str, virtualized: bool | None = False, branch: str | None = "main"
    ):
        """
        Given a dataset, push a new commit alongisde the data to the icechunk store

        Parameters
        ----------
        ds : xr.Dataset
            Dataset to be commited to the icechunk store.
        commit : str
            Commit message that will accompany the dataset push.
        virtualized : bool | None, optional
            Designates if the dataset to be written is virtualized. Affects
            how it's written to icechunk. By default False
        branch : str | None, optional
            Icechunk repo branch to be pushed. By default "main".
        """
        session = self.create_session(read_only=False, branch=branch)
        if virtualized:
            ds.virtualize.to_icechunk(session.store)
        else:
            to_icechunk(ds, session)
        snapshot = session.commit(commit)
        print(f"Dataset is uploaded. Commit: {snapshot}")

    def append_virt_data_to_store(
        self, vds: xr.Dataset, append_dim: str, commit: str, branch: str | None = "main"
    ):
        """
        Add new data to the store

        Given a virtualized dataset, push a new commit to append
        data to an existing icechunk store. The data will be
        appended on a specified dimension.

        Parameters
        ----------
        vds : xr.Dataset
            The virtualized dataset to be appended to the
            existing icechunk store.
        append_dim : str
            What dimension the dataset will be appended on. Likely
            time or year, etc.
        commit : str
            Commit message that will accompany the dataset addition.
        branch : str | None, optional
            Icechunk repo branch to be pushed. By default "main".
        """
        session = self.create_session(read_only=False, branch=branch)
        vds.virtualize.to_icechunk(session.store, append_dim=append_dim)
        snapshot = session.commit(commit)
        print(f"Dataset has been appended on the {append_dim} dimension. Commit: {snapshot}")

    def retrieve_and_convert_to_tif(
        self,
        dest: str | Path,
        var_name: str = None,
        branch: str | None = "main",
        compress: str = "lzw",
        tiled: bool = True,
        minx: float | None = None,
        miny: float | None = None,
        maxx: float | None = None,
        maxy: float | None = None,
        profile_kwargs: dict[Any, Any] = None,
    ) -> None:
        """A function to retrieve a raster icechunk dataset and download as a tif.

        Parameters
        ----------
        dest : str | Path
            Destination file path for tiff
        var_name : str, optional
            Name of xarray variable to be used for raster data, by default None
        branch : str | None, optional
            Icechunk repo branch to be opened, by default "main"
        compress : str, optional
            Specify a compression type for raster, by default "lzw"
        tiled : bool, optional
           Specify if raster should be tiled or not. Cloud-Optimized Geotiffs (COG) must be tiled, by default True
        minx : float | None, optional
            Specify a bounding box minimum x. Must have all [minx, miny, maxx, maxy] specified, by default None
        miny : float | None, optional
           Specify a bounding box minimum y. Must have all [minx, miny, maxx, maxy] specified, by default None
        maxx : float | None, optional
           Specify a bounding box maximum x. Must have all [minx, miny, maxx, maxy] specified, by default None
        maxy : float | None, optional
            Specify a bounding box maximum x. Must have all [minx, miny, maxx, maxy] specified, by default None
        profile_kwargs : dict[Any, Any], optional
            Any additional profile keywords accepted by GDAL geotiff driver
            (https://gdal.org/en/stable/drivers/raster/gtiff.html#creation-options), by default None


        Raises
        ------
        AttributeError
            If an xarray dataset does not have a "band" attribute in coordinates, the file is not deemed a raster
            and will raise error.
        """
        ds = self.retrieve_dataset(self.repo, read_only=True, branch=branch)

        try:
            _ = ds.coords.band
        except AttributeError as e:
            raise AttributeError("Dataset needs a 'band' coordinate to export geotiff") from e

        # infer variable name if none provided - MAY HAVE UNEXPECTED RESULTS
        if not var_name:
            var_name = self._infer_var_name_for_geotiff(list(ds.data_vars.variables))

        # clip to window
        if minx and miny and maxx and maxy:
            subset = ds.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
            subset[var_name].rio.to_raster(dest, compress=compress, tiled=tiled, **profile_kwargs)
            del subset
            print(f"Saved clipped window to {dest}")

        else:
            ds[var_name].rio.to_raster(dest, compress=compress, tiled=tiled, **profile_kwargs)
            del ds
            print(f"Saved dataset to {dest}")

    def _infer_var_name_for_geotiff(self, variable_list: list) -> str:
        """Infer a variable name for saving a geotiff from xarray variables

        Picks the first variable that isn't 'spatial_ref'. In zarr, 'spatial_ref' from CRS is moved
        from coordinates to variables. We want a variable that is not it.
        This arbitarily picks the first variable.

        Parameters
        ----------
        variable_list : list
            Output of list(ds.data_vars.variables)

        Returns
        -------
        str
            Variable name to use for geotif generation
        """
        if "spatial_ref" in variable_list:
            variable_list.remove("spatial_ref")
        var_name = variable_list[0]
        warnings.warn(
            UserWarning,
            f"Inferring xarray variable name {var_name} for raster data. This may have unintended consequences."
            "Open dataset separately to check variable names to insure correct output.",
            stacklevel=2,
        )
        return var_name


@staticmethod
def set_up_virtual_chunk_container(bucket: str, region: str) -> ic.VirtualChunkContainer:
    """
    Create a virtual chunk container from a mapping

    Given an S3 bucket/region, generate and return a VirtualChunkContainer
    so Icechunk can point to virtualized data inside S3 buckets.

    Parameters
    ----------
    bucket : str
        The S3 bucket the virtual chunk points to.
    region : str
        The region of the S3 bucket.

    Returns
    -------
    ic.VirtualChunkContainer
        A definition of a virtual chunk that the icechunk repo
        uses to define access to virtualized data.
    """
    return ic.VirtualChunkContainer(
        name=bucket, url_prefix=f"s3://{bucket}/", store=ic.s3_store(region=region)
    )


@staticmethod
def get_icechunk_data(repo: NGWPCLocations) -> xr.Dataset:
    """Return data from a designated NGWPCLocations Icechunk repo"""
    return IcechunkS3Repo(location=repo.path).retrieve_dataset()
