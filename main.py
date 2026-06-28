import uvicorn

from config.settings import settings


if __name__ == "__main__":
    uvicorn.run(
        "backend.api.app:app",
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        reload=settings.debug and settings.workers == 1,
    )
