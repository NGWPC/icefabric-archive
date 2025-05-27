import uvicorn
from fastapi import FastAPI, status
from pydantic import BaseModel

from .routers.streamflow_observations.router import api_router as streamflow_api_router

app = FastAPI(
    title="Icefabric API",
    description="API for accessing iceberg or icechunk data from EDFS services",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""

    status: str = "OK"


# Include routers
app.include_router(streamflow_api_router, prefix="/v1")


@app.head(
    "/health",
    tags=["Health"],
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheck,
)
def get_health() -> HealthCheck:
    """Returns a HeatlhCheck for the server"""
    return HealthCheck(status="OK")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
