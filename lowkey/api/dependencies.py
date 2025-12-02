from typing import Annotated
from .. import FilesystemStorage
from ..settings import ScraperSettings, ParserSettings
from fastapi import Depends
from ..storage import MinioStorage


def get_scraper_settings():
    settings = ScraperSettings()
    return settings


def get_parser_settings():
    settings = ParserSettings()
    return settings


def get_storage():
    settings = get_scraper_settings()
    if settings.storage_type == "local":
        storage = FilesystemStorage(base_path="./minio")
    else:
        storage = MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    return storage


Storage = Annotated[MinioStorage, Depends(get_storage)]
ScraperSettings = Annotated[ScraperSettings, Depends(get_scraper_settings)]
ParserSettings = Annotated[ParserSettings, Depends(get_parser_settings)]
