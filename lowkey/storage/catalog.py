import fnmatch
import json
from typing import Literal
from .client import Storage
from datetime import datetime, UTC
from ..utils import generate_run_id
from ..duck import query


class Catalog:
    """
    catalog/
        bronze/
            {project_name}/
                {scraper_name}/
                    dt={YYYY}-{MM}-{DD}/
                        EnIO51QmBi.json
                        NoHLgxhoQi.parquet
    """

    def __init__(
        self,
        input_storage: Storage,
        output_storage: Storage,
        project_name: str,
        scraper_name: str,
        layer: Literal["bronze", "silver"],
    ) -> None:
        self.input_storage = input_storage
        self.output_storage = output_storage
        self.project_name = project_name
        self.scraper_name = scraper_name
        self.layer = layer
        self.run_date = datetime.now(UTC)

    async def save(self, key: str):
        metadata = {
            "project_name": self.project_name,
            "scraper_name": self.scraper_name,
            "dt": self.run_date.strftime("%Y-%m-%d"),
            "ts": self.run_date.strftime("%Y%m%d%H%M%S"),
            "key": key,
        }
        metadata_file_name = generate_run_id()
        catalog_key = (
            f"{self.catalog_date_path(self.run_date)}/{metadata_file_name}.json"
        )
        await self.output_storage.save(
            catalog_key, json.dumps(metadata).encode("utf-8")
        )

    @property
    def catalog_scraper_path(self):
        return f"catalog/{self.layer}/{self.project_name}/{self.scraper_name}"

    def catalog_date_path(self, date: datetime):
        return f"{self.catalog_scraper_path}/dt={date.strftime('%Y-%m-%d')}"

    @property
    def input_path(self):
        return f"{self.layer}/{self.project_name}/{self.scraper_name}"

    async def list_files(
        self,
        key: str,
        pattern: str,
    ) -> list[str]:
        prefix = key.lstrip("/")
        sql_query = f"""
        SELECT key
        FROM (
            SELECT * FROM read_parquet('s3://prod/{self.catalog_scraper_path}/*.parquet', hive_partitioning=1)
            UNION ALL
            SELECT * FROM read_json('s3://prod/{self.catalog_scraper_path}/*.json', format='auto')
        )
        WHERE starts_with(key, '{prefix}')
        OR starts_with(key, '/{prefix}')
        """
        file_names = [file["key"] for file in query(sql_query)]

        names = []
        for name in file_names:
            rel = name[len(prefix) :].lstrip("/")
            if fnmatch.fnmatch(rel, pattern):
                names.append(name)
        return names

    async def generate(self):
        file_names = await self.input_storage.list_files(self.input_path, "*")
        for file_name in file_names:
            await self.save(file_name)
        await self.input_storage.close()
        await self.output_storage.close()

    async def compact_catalog_files(self): ...
