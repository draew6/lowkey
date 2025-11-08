import asyncio
from typing import Literal
import typer
from fastlet.cli.run import export_file, start as fastlet_start
from ..settings import ScraperSettings
from ..storage import FilesystemStorage, MinioStorage, Catalog


app = typer.Typer()


@app.command()
def start():
    fastlet_start()
    export_file("requirements-base.txt", src_dir=__file__, mode="a")


@app.command()
def generate_catalog(
    scraper_name: str = typer.Argument(None),
    input_storage: Literal["local", "minio"] = typer.Option(
        "local", help="Storage type to use (local or minio)"
    ),
    output_storage: Literal["local", "minio"] = typer.Option(
        "local", help="Storage type to use (local or minio)"
    ),
):
    settings = ScraperSettings()
    input_storage_to_use = (
        FilesystemStorage("./minio")
        if input_storage == "local"
        else MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    )
    output_storage_to_use = (
        FilesystemStorage("./minio")
        if output_storage == "local"
        else MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    )
    catalog = Catalog(
        input_storage_to_use,
        output_storage_to_use,
        settings.project_name,
        scraper_name,
        "bronze",
    )
    asyncio.run(catalog.generate())


@app.command()
def compact_catalog(
    scraper_name: Literal["postings", "details"] = typer.Argument(None),
    input_storage: Literal["local", "minio"] = typer.Option(
        "local", help="Storage type to use (local or minio)"
    ),
    output_storage: Literal["local", "minio"] = typer.Option(
        "local", help="Storage type to use (local or minio)"
    ),
):
    settings = ScraperSettings()
    input_storage_to_use = (
        FilesystemStorage("./minio")
        if input_storage == "local"
        else MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    )
    output_storage_to_use = (
        FilesystemStorage("./minio")
        if output_storage == "local"
        else MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    )
    catalog = Catalog(
        input_storage_to_use,
        output_storage_to_use,
        settings.project_name,
        scraper_name,
        "bronze",
    )
    asyncio.run(catalog.compact_catalog_files())


@app.command()
def compact_catalog(
    scraper_name: Literal["postings", "details"] = typer.Argument(None),
    input_storage: Literal["local", "minio"] = typer.Option(
        "local", help="Storage type to use (local or minio)"
    ),
    output_storage: Literal["local", "minio"] = typer.Option(
        "local", help="Storage type to use (local or minio)"
    ),
):
    settings = ScraperSettings()
    input_storage_to_use = (
        FilesystemStorage("./minio")
        if input_storage == "local"
        else MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    )
    output_storage_to_use = (
        FilesystemStorage("./minio")
        if output_storage == "local"
        else MinioStorage(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_bucket_name,
        )
    )
    catalog = Catalog(
        input_storage_to_use,
        output_storage_to_use,
        settings.project_name,
        scraper_name,
        "bronze",
    )
    asyncio.run(catalog.compact_catalog_files())


def main():
    app()
