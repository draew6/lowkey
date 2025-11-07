from typing import Annotated
from ..settings import ScraperSettings as Settings
from fastapi import Depends
from ..storage import MinioStorage


def get_scraper_settings():
    settings = Settings()
    return settings


def get_storage():
    settings = get_scraper_settings()
    storage = MinioStorage(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        settings.minio_bucket_name,
    )
    return storage


Storage = Annotated[MinioStorage, Depends(get_storage)]
ScraperSettings = Annotated[Settings, Depends(get_scraper_settings)]
