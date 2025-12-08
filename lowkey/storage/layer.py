import dataclasses
import json
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from .client import Storage, File
from .info import ScraperInfo, ParserInfo
from .catalog import Catalog

EMPTY_FILE = b""


class Layer(ABC):
    def __init__(
        self,
        storage: Storage,
        project_name: str,
        scraper_name: str,
        run_id: str,
    ) -> None:
        self.storage = storage
        self.project_name = project_name
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.run_date = datetime.now(UTC)

    @staticmethod
    @abstractmethod
    def _create_scraper_path(project_name: str, scraper_name: str) -> str: ...

    @property
    def _scraper_path(self) -> str:
        return self._create_scraper_path(self.project_name, self.scraper_name)

    @property
    @abstractmethod
    def _run_path(self) -> str: ...

    @abstractmethod
    async def save(self, *args: any, **kwargs: any) -> None: ...

    @abstractmethod
    async def remove(self, *args: any, **kwargs: any) -> None: ...

    @property
    @abstractmethod
    def files_path(self) -> str: ...

    async def _mark_run(self, status: str):
        key = f"{self._run_path}/_{status}"
        await self.storage.save(key, EMPTY_FILE)

    async def mark_run_as_failed(self) -> None:
        await self._mark_run("FAILED")

    async def mark_run_as_completed(self) -> None:
        await self._mark_run("COMPLETED")

    async def mark_run_as_started(self) -> None:
        await self._mark_run("STARTED")

    async def create_run_info(
        self,
        RunInfo,
    ):
        run_info = json.dumps(dataclasses.asdict(RunInfo)).encode("utf-8")
        key = f"{self._run_path}/run.json"
        await self.storage.save(key, run_info)

    async def create_actor_info(self, actor: ScraperInfo | ParserInfo):
        # actor_info = str(actor.__dict__).encode()
        actor_info = json.dumps(dataclasses.asdict(actor)).encode("utf-8")
        key = f"{self._run_path}/actor.json"
        await self.storage.save(key, actor_info)

    async def heartbeat(self) -> None:
        key = f"{self._run_path}/heartbeats/{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}.hb"
        await self.storage.save(key, EMPTY_FILE)

    async def load_run_files(self, pattern: str) -> list[File]:
        file_names = await self.storage.list_files(self.files_path, pattern)
        return [file async for file in self.storage.load_files(file_names)]

    @classmethod
    async def list_run_ids(
        cls, project_name: str, scraper_name: str, catalog: Catalog
    ) -> list[str]:
        key = cls._create_scraper_path(project_name, scraper_name)
        files = await catalog.list_files(key, pattern="*_STARTED")
        run_ids = []
        for file in files:
            run_id = file.split("run=")[1].split("/")[0]
            run_ids.append(run_id)
        return run_ids


class BronzeLayer(Layer):
    """
    bronze/
        {project_name}/
            {scraper_name}/
                {YYYY}/{MM}/{DD}/ # TODO: should be same as silver?
                    run={run_id}/
                        _STARTED
                        _COMPLETED
                        heartbeats/
                            20240101120000.hb
                            20240101120100.hb
                        artifacts/
                            {identifier}={id_value}/ # e.g. posting_id=12345 or page=1
                                request.curl  #curlify
                                request.meta.json # python request object serialized
                                request.crawlee.json # crawlee request object serialized
                                response.body.zst # zstandard
                                response.meta.json # python response object serialized
                        run.json
                        actor.json

    """

    def __init__(
        self,
        storage: Storage,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        catalog: Catalog,
    ) -> None:
        super().__init__(storage, project_name, scraper_name, run_id)
        self.identifier = identifier
        self.catalog = catalog

    @staticmethod
    def _create_scraper_path(project_name: str, scraper_name: str) -> str:
        return f"bronze/{project_name}/{scraper_name}"

    @property
    def _run_path(self):
        # return f"bronze/{self.project_name}/{self.scraper_name}/{datetime.now(UTC).strftime('%Y/%m/%d')}/run={self.run_id}"
        return f"{self._scraper_path}/{self.run_date.strftime('%Y/%m/%d')}/run={self.run_id}"

    async def save(self, name: str, id_value: str, content: bytes) -> None:
        key = f"{self.files_path}/{self.identifier}={id_value}/{name}"
        await self.storage.save(key, content)
        await self.catalog.save(key)

    async def remove(self, name: str, id_value: str) -> None:
        key = f"{self.files_path}/{self.identifier}={id_value}/{name}"
        await self.storage.delete(key)

    @property
    def files_path(self) -> str:
        return f"{self._run_path}/artifacts"

    async def _mark_run(self, status: str):
        key = f"{self._run_path}/_{status}"
        await self.storage.save(key, EMPTY_FILE)
        await self.catalog.save(key)

    async def create_run_info(
        self,
        RunInfo,
    ):
        run_info = json.dumps(dataclasses.asdict(RunInfo)).encode("utf-8")
        key = f"{self._run_path}/run.json"
        await self.storage.save(key, run_info)
        await self.catalog.save(key)

    async def create_actor_info(self, actor: ScraperInfo | ParserInfo):
        # actor_info = str(actor.__dict__).encode()
        actor_info = json.dumps(dataclasses.asdict(actor)).encode("utf-8")
        key = f"{self._run_path}/actor.json"
        await self.storage.save(key, actor_info)
        await self.catalog.save(key)

    async def heartbeat(self) -> None:
        key = f"{self._run_path}/heartbeats/{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}.hb"
        await self.storage.save(key, EMPTY_FILE)
        await self.catalog.save(key)

    async def load_run_files(self, pattern: str) -> list[File]:
        file_names = await self.catalog.list_files(self.files_path, pattern)
        return [file async for file in self.storage.load_files(file_names)]


class SilverLayer(Layer):
    """
    silver/
        {project_name}/
            {scraper_name}/
                dt={YYYY}-{MM}-{DD}/
                    run={run_id}/
                        part-0000.parquet
                        part-0001.parquet
                        _STARTED
                        _COMPLETED
                        run.json
                        actor.json
    """

    @staticmethod
    def _create_scraper_path(project_name: str, scraper_name: str) -> str:
        return f"silver/{project_name}/{scraper_name}"

    @property
    def _run_path(self) -> str:
        # return f"silver/{self.project_name}/{self.scraper_name}/dt={datetime.now(UTC).strftime('%Y-%m-%d')}/run={self.run_id}"
        return f"{self._scraper_path}/dt={self.run_date.strftime('%Y-%m-%d')}/run={self.run_id}"

    async def save(self, name: str, content: bytes) -> None:
        key = f"{self._run_path}/{name}"
        await self.storage.save(key, content)

    async def remove(self, name: str) -> None:
        key = f"{self._run_path}/{name}"
        await self.storage.delete(key)

    @property
    def files_path(self) -> str:
        return self._run_path
