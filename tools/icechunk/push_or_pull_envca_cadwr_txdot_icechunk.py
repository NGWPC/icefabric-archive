import icechunk
import xarray as xr
from icechunk.xarray import to_icechunk

bucket = "ngwpc-hydrofabric"
prefix = "envca_cadwr_txdot_observations"
storage_config = icechunk.s3_storage(bucket=bucket, prefix=prefix, region="us-east-1", from_env=True)
try:
    repo = icechunk.Repository.create(storage_config)
    session = repo.writable_session("main")
    ds = xr.open_zarr("envca_cadwr_txdot.zarr")
    to_icechunk(ds, session)
    snapshot = session.commit("Uploaded all ENVCA, CADWR and TXDOT gages to the store")
    print(f"All data is uploaded. Commit: {snapshot}")
except icechunk.IcechunkError as e:
    if "repositories can only be created in clean prefixes" in e.message:
        print("envca_cadwr_txdot_observations icechunk store already exists. Pulling it down now...")
        repo = icechunk.Repository.open(storage_config)
        session = repo.writable_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
        print(ds)
    else:
        print(f"Unexpected Icechunk error: {e}")
