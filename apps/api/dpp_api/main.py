"""DPP API - FastAPI Application Entry Point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dpp_api.routers import health, runs

app = FastAPI(
    title="DPP API",
    description="Decision Pack Platform - Agent-Centric API Platform",
    version="0.4.2.2",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware (adjust for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["health"])
app.include_router(runs.router)  # API-01: Runs endpoints


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {
        "service": "DPP API",
        "version": "0.4.2.2",
        "status": "running",
    }
