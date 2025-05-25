from app.routers.streamflow_observations.router import api_router as streamflow_api_router
from fastapi import FastAPI

app = FastAPI(
    title="Icefabric API",
)

app.include_router(streamflow_api_router, prefix="/v1")
