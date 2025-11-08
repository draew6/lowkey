import asyncio
import fnmatch
import json
from io import BytesIO
from typing import Literal
import pandas as pd
from duckdb import IOException
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
                      json/
                        EnIO51QmBi.json
                      parquet/
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
            f"{self.catalog_date_path(self.run_date)}/json/{metadata_file_name}.json"
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
        sql_query_parquet = f"""
        SELECT key
        FROM read_parquet('s3://prod/{self.catalog_scraper_path}/dt=*/parquet/*.parquet', hive_partitioning=1)
        WHERE starts_with(key, '{prefix}')
        OR starts_with(key, '/{prefix}')
        """
        sql_query_json = f"""
        SELECT key FROM read_json('s3://prod/{self.catalog_scraper_path}/dt=*/json/*.json', format='auto')
        WHERE starts_with(key, '{prefix}')
        OR starts_with(key, '/{prefix}')
        """
        try:
            parquet_file_names = [file["key"] for file in query(sql_query_parquet)]
        except IOException:
            parquet_file_names = []
        try:
            json_file_names = [file["key"] for file in query(sql_query_json)]
        except IOException:
            json_file_names = []
        names = []
        for name in parquet_file_names + json_file_names:
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

    async def compact_catalog_files(self):
        batch_number = 0
        while True:
            file_names_batch = await self.output_storage.list_files(
                f"{self.catalog_scraper_path}", "*.json", 1000
            )
            files_batch = await self.output_storage.load_files(file_names_batch)
            rows = [json.loads(file.content.decode("utf-8")) for file in files_batch]
            df = pd.DataFrame(rows)
            buf = BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")  # type: ignore[arg-type]
            parquet_file_name = f"{generate_run_id()}-{batch_number:06d}.parquet"
            await self.output_storage.save(
                f"{self.catalog_date_path(datetime.now(UTC))}/parquet/{parquet_file_name}",
                buf.getvalue(),
            )
            delete_tasks = [
                self.output_storage.delete(file_name) for file_name in file_names_batch
            ]
            await asyncio.gather(*delete_tasks)
            if len(file_names_batch) < 1000:
                break
            batch_number += 1
        await self.input_storage.close()
        await self.output_storage.close()
