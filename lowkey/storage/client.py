import glob
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from miniopy_async import Minio
import io
import fnmatch
import os


@dataclass
class File:
    name: str
    content: bytes


class Storage(ABC):
    @abstractmethod
    async def save(self, key: str, value: bytes) -> None: ...

    @abstractmethod
    async def load_files(self, file_names: list[str]) -> list[File]: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def list_files(self, key: str, pattern: str) -> list[str]: ...

    @abstractmethod
    async def delete(self, key: str) -> None: ...


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

    async def load_files(self, file_names: list[str]) -> list[File]:
        data: list[File] = []
        for file in file_names:
            with open(file, "rb") as f:
                data.append(File(name=file, content=f.read()))

        return data

    async def delete(self, key: str) -> None:
        path = os.path.join(self.base_path, key)
        if os.path.exists(path):
            os.remove(path)

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

    async def load_files(self, file_names: list[str]) -> list[File]:
        async def get_one_file(object_name: str) -> File:
            async with await self.client.get_object(
                self.bucket_name, object_name
            ) as resp:
                content = await resp.read()
            return File(name=object_name, content=content)

        tasks = [get_one_file(file) for file in file_names]

        if not tasks:
            return []

        results = await asyncio.gather(*tasks)
        return results

    async def delete(self, key: str) -> None:
        await self.client.remove_object(self.bucket_name, key)

    async def close(self) -> None:
        await self.client.close_session()
