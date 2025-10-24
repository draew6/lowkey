import dataclasses
import glob
import json
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, UTC
from crawlee.crawlers import BeautifulSoupCrawler
from miniopy_async import Minio
import io
import fnmatch
import os
import sys
from importlib.metadata import version, PackageNotFoundError

from .models.client import APIClient
from .models.user import User

EMPTY_FILE = b""


@dataclass
class File:
    name: str
    content: bytes


class Storage(ABC):
    @abstractmethod
    async def save(self, key: str, value: bytes) -> None: ...

    @abstractmethod
    async def load_files(self, key: str, pattern: str) -> list[File]: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def list_files(self, key: str, pattern: str) -> list[str]: ...


class FilesystemStorage(Storage):
    def __init__(self, base_path: str) -> None:
        self.base_path = base_path

    async def save(self, key: str, value: bytes) -> None:
        path = os.path.join(self.base_path, key)
        dir_path = "/".join(path.split("/")[:-1])
        os.makedirs(dir_path, exist_ok=True)
        with open(path, "wb") as f:
            f.write(value)

    async def list_files(self, key: str, pattern: str) -> list[str]:
        path = os.path.join(self.base_path, key, pattern)
        files = glob.glob(path)
        return files

    async def load_files(self, key: str, pattern: str = "*") -> list[File]:
        files = await self.list_files(key, pattern)
        data: list[File] = []
        for file in files:
            with open(file, "rb") as f:
                data.append(File(name=file, content=f.read()))

        return data

    async def close(self) -> None:
        pass


class MinioStorage(Storage):
    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, bucket_name: str
    ) -> None:
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=True,
        )
        self.bucket_name = bucket_name

    async def save(self, key: str, value: bytes) -> None:
        await self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=key,
            data=io.BytesIO(value),
            length=len(value),
        )

    async def list_files(self, key: str, pattern: str) -> list[str]:
        objects = self.client.list_objects(
            bucket_name=self.bucket_name, prefix=key, recursive=True
        )
        prefix = key.lstrip("/")
        names = []
        async for obj in objects:
            name = obj.object_name
            rel = name[len(prefix) :].lstrip("/")
            if fnmatch.fnmatch(rel, pattern):
                names.append(name)
        return names

    async def load_files(self, key: str, pattern: str = "*") -> list[File]:
        async def get_one_file(object_name: str) -> File:
            async with await self.client.get_object(
                self.bucket_name, object_name
            ) as resp:
                content = await resp.read()
            return File(name=object_name, content=content)

        files = await self.list_files(key, pattern)
        tasks = [get_one_file(file) for file in files]

        if not tasks:
            return []

        results = await asyncio.gather(*tasks)
        return results

    async def close(self) -> None:
        await self.client.close_session()


def get_module_version(module_name: str) -> str:
    try:
        return version(module_name)
    except PackageNotFoundError:
        return "unknown"


def get_python_version() -> str:
    return ".".join(map(str, sys.version_info[:3]))


def get_git_commit() -> str | None:
    return os.getenv("SOURCE_COMMIT")


@dataclass
class ActorInfo:
    name: str
    git_commit: str
    python_version: str
    fastlet_version: str

    @classmethod
    def get_info(cls, name: str):
        return cls(
            name=name,
            git_commit=get_git_commit(),
            python_version=get_python_version(),
            fastlet_version=get_module_version("fastlet"),
        )


@dataclass
class ScraperInfo(ActorInfo):
    crawlee_version: str
    playwright_version: str
    blackcat_version: str

    @classmethod
    def get_info(cls, name: str):
        parent = cls.__mro__[1]  # ActorInfo
        base = parent.get_info(name)
        return cls(
            name=name,
            git_commit=base.git_commit,
            python_version=base.python_version,
            fastlet_version=base.fastlet_version,
            crawlee_version=get_module_version("crawlee"),
            playwright_version=get_module_version("playwright"),
            blackcat_version=get_module_version("blackcat"),
        )


@dataclass
class RunInfo:
    requested_by: str
    requested_at: str
    actor_name: str
    git_commit: str

    @classmethod
    def get_info(cls, requested_by: str, actor_name: str):
        return cls(
            requested_by=requested_by,
            requested_at=datetime.now(UTC).isoformat(),
            actor_name=actor_name,
            git_commit=get_git_commit(),
        )


@dataclass
class ParserInfo(ActorInfo):
    bs4_version: float
    pydantic_version: float


class Layer(ABC):
    def __init__(
        self, storage: Storage, project_name: str, scraper_name: str, run_id: str
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
        return await self.storage.load_files(self.files_path, pattern)

    @classmethod
    async def list_run_ids(
        cls, project_name: str, scraper_name: str, storage: Storage
    ) -> list[str]:
        key = cls._create_scraper_path(project_name, scraper_name)
        files = await storage.list_files(key, pattern="run=*/_STARTED")
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
    ) -> None:
        super().__init__(storage, project_name, scraper_name, run_id)
        self.identifier = identifier

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

    @property
    def files_path(self) -> str:
        return f"{self._run_path}/artifacts"


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

    @property
    def files_path(self) -> str:
        return self._run_path


class ScraperStorage:
    def __init__(
        self,
        storage: Storage,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        api_client: APIClient,
    ) -> None:
        self.bronze = BronzeLayer(
            storage, project_name, scraper_name, run_id, identifier
        )
        self.silver = SilverLayer(storage, project_name, scraper_name, run_id)
        self.api_client = api_client

    async def start_run(
        self, crawler: BeautifulSoupCrawler, run_info: RunInfo, actor_info: ScraperInfo
    ):
        await self.bronze.create_run_info(run_info)
        await self.bronze.create_actor_info(actor_info)
        await self.bronze.mark_run_as_started()
        await crawler.run()
        await self.bronze.mark_run_as_completed()

        await self.bronze.storage.close()
        await self.silver.storage.close()
        # TODO: ak session je vylučena z poolu, tak dostanem ju tu aby som ju mohol updatnut?
        users = crawler._session_pool.create_users_from_sessions()
        await User.update_users(self.api_client, users)
        await crawler._request_manager.drop()
