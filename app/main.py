from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import items, users
from app.core.config import settings

def create_application() -> FastAPI:
    """Create FastAPI application."""
    application = FastAPI(
        title=settings.PROJECT_NAME,
        description=settings.PROJECT_DESCRIPTION,
        version=settings.VERSION,
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Set up CORS middleware
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    application.include_router(users.router, prefix=settings.API_V1_STR)
    application.include_router(items.router, prefix=settings.API_V1_STR)

    @application.get("/")
    def root():
        """Root endpoint."""
        return {"message": "Welcome to Modular FastAPI Project!"}

    return application


app = create_application()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

