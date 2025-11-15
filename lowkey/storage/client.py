import glob
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
import duckdb
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
    async def list_files(
        self, key: str, pattern: str, limit: int = None
    ) -> list[str]: ...

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

    async def list_files(self, key: str, pattern: str, limit: int = None) -> list[str]:
        path = os.path.join(self.base_path, key, pattern)
        files = glob.glob(path)
        if limit:
            return files[:limit]
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

    async def list_files(self, key: str, pattern: str, limit: int = None) -> list[str]:
        objects = self.client.list_objects(
            bucket_name=self.bucket_name, prefix=key, recursive=True
        )
        prefix = key.lstrip("/")
        names = []
        counter = 0
        async for obj in objects:
            name = obj.object_name
            rel = name[len(prefix) :].lstrip("/")
            if fnmatch.fnmatch(rel, pattern):
                names.append(name)
                counter += 1
            # page has 1000 items
            if limit is not None and counter >= limit:
                break
        return names

    async def load_files(self, file_names: list[str]) -> list[File]:
        async def get_one_file(object_name: str) -> File:
            async with await self.client.get_object(
                self.bucket_name, object_name
            ) as resp:
                content = await resp.read()
            return File(name=object_name, content=content)

        if not file_names:
            return []

        results: list[File] = []
        BATCH_SIZE = 1000

        for start in range(0, len(file_names), BATCH_SIZE):
            batch = file_names[start : start + BATCH_SIZE]
            tasks = [get_one_file(name) for name in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

            # Wait 1s between batches if more remain
            if start + BATCH_SIZE < len(file_names):
                await asyncio.sleep(1)

        return results

    async def delete(self, key: str) -> None:
        await self.client.remove_object(self.bucket_name, key)

    async def close(self) -> None:
        await self.client.close_session()


class DuckLakeStorage(MinioStorage):
    def __init__(
        self,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_bucket_name: str,
        duck_pg_host: str,
        duck_pg_user: str,
        duck_pg_password: str,
        duck_pg_dbname: str,
        duck_pg_port: str,
        table: str,
    ):
        super().__init__(
            minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name
        )

        self.connection = self._load_duck_db(
            minio_endpoint,
            minio_access_key,
            minio_secret_key,
            minio_bucket_name,
            duck_pg_host,
            duck_pg_user,
            duck_pg_password,
            duck_pg_dbname,
            duck_pg_port,
        )
        self.table = table
        self.prefix = f"s3://{minio_bucket_name}/"

    @staticmethod
    def _load_duck_db(
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_bucket_name: str,
        duck_pg_host: str,
        duck_pg_user: str,
        duck_pg_password: str,
        duck_pg_dbname: str,
        duck_pg_port: str,
    ):
        con = duckdb.connect()
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute("INSTALL ducklake;")
        con.execute("LOAD ducklake;")
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")

        con.execute(f"""
    CREATE OR REPLACE SECRET minio_env (
      TYPE s3,
      PROVIDER config,
      KEY_ID '{minio_access_key}',
      SECRET '{minio_secret_key}',
      ENDPOINT '{minio_endpoint}',
      URL_STYLE 'path',
      USE_SSL false,
      SCOPE 's3://{minio_bucket_name}/'
    );
    """)
        con.execute(f"""
        ATTACH 'ducklake:postgres:host={duck_pg_host} user={duck_pg_user} password={duck_pg_password} dbname={duck_pg_dbname} port={duck_pg_port}' AS lake (DATA_PATH 's3://{minio_bucket_name}/lake/');
        USE lake;
        """)

        return con

    def _query[T](self, query: str, result_type: T = dict) -> list[T]:
        con = self.connection
        cur = con.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = [result_type(**dict(zip(columns, row))) for row in rows]
        return result

    def add_file(self, key: str):
        sql = f"CALL ducklake_add_data_files('lake', '{self.table}', '{self.prefix}{key}');"
        self.connection.execute(sql)

    def _list_files(self):
        sql = f"CALL ducklake_list_files('lake', '{self.table}');"
        return self._query(sql)

    async def save(self, key: str, value: bytes) -> None:
        await super().save(key, value)
        if key.endswith(".parquet"):
            self.add_file(key)

    # async def list_files(self, key: str, pattern: str, limit: int = None) -> list[str]:
    #     file_names = [
    #         file["data_file"].replace(self.prefix, "")
    #         for file in self._list_files()
    #         if file["data_file"].startswith(f"{self.prefix}{key}")
    #     ]
    #     prefix = key.lstrip("/")
    #     names = []
    #     counter = 0
    #     for file_name in file_names:
    #         rel = file_name[len(prefix) :].lstrip("/")
    #         if fnmatch.fnmatch(rel, pattern):
    #             names.append(file_name)
    #             counter += 1
    #         # page has 1000 items
    #         if limit is not None and counter >= limit:
    #             break
    #     return names

    async def close(self) -> None:
        await super().close()
        self.connection.close()
