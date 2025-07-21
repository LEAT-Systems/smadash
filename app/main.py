from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes import users
from .api.utils.config import settings
from .api.db.database import instantiate_db

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
    #application.include_router(items.router, prefix=settings.API_V1_STR)

    @application.get("/")
    def root():
        """Root endpoint."""
        return {"message": "Welcome to Modular FastAPI Project!"}

    return application


app = create_application()
instantiate_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

