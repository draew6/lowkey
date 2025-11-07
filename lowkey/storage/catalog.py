import fnmatch
import json

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
        storage: Storage,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
    ) -> None:
        self.storage = storage
        self.project_name = project_name
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.run_date = datetime.now(UTC)
        self.identifier = identifier

    async def save(self, key: str):
        metadata = {
            "project_name": self.project_name,
            "scraper_name": self.scraper_name,
            "dt": self.run_date.strftime("%Y-%m-%d"),
            "ts": self.run_date.strftime("%Y%m%d%H%M%S"),
            "run_id": self.run_id,
            "identifier": self.identifier,
            "id_value": key.split(f"{self.identifier}=")[1].split("/")[0],
            "key": key,
        }
        metadata_file_name = generate_run_id()
        catalog_key = f"{self.date_path(self.run_date)}/{metadata_file_name}.json"
        await self.storage.save(catalog_key, json.dumps(metadata).encode("utf-8"))

    async def scraper_path(self):
        return f"catalog/bronze/{self.project_name}/{self.scraper_name}"

    async def date_path(self, date: datetime):
        return f"{await self.scraper_path()}/dt={date.strftime('%Y-%m-%d')}"

    async def list_files(
        self,
        key: str,
        pattern: str,
    ) -> list[str]:
        prefix = key.lstrip("/")
        sql_query = f"""
        SELECT key
        FROM (
            SELECT * FROM read_parquet('s3://prod/{self.scraper_path()}/*.parquet', hive_partitioning=1)
            UNION ALL
            SELECT * FROM read_json('s3://prod/{self.scraper_path()}/*.json', format='auto')
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

    async def compact_catalog_files(self): ...
