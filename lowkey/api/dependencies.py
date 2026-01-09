from typing import Annotated
from ..settings import ScraperSettings, ParserSettings
from fastapi import Depends
from ..storage import MinioStorage, FilesystemStorage, Storage as BaseStorage
from ..storage.client import DuckLakeStorage


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


Storage = Annotated[BaseStorage, Depends(get_storage)]
ScraperSettings = Annotated[ScraperSettings, Depends(get_scraper_settings)]
ParserSettings = Annotated[ParserSettings, Depends(get_parser_settings)]


async def get_ducklake(parser_settings: ParserSettings):
    client = DuckLakeStorage(
        parser_settings.minio_endpoint,
        parser_settings.minio_access_key,
        parser_settings.minio_secret_key,
        parser_settings.minio_bucket_name,
        parser_settings.duck_pg_host,
        parser_settings.duck_pg_user,
        parser_settings.duck_pg_password,
        parser_settings.duck_pg_dbname,
        str(parser_settings.duck_pg_port),
    )
    try:
        yield client
    finally:
        await client.close()


Lake = Annotated[DuckLakeStorage, Depends(get_ducklake)]
