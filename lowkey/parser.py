import inspect
import math
from io import BytesIO
from typing import Callable, get_type_hints, AsyncIterator
from . import generate_run_id
from .storage import RunInfo
from .storage.layer import SilverLayer, BronzeLayer
from .storage.client import Storage
from .storage.catalog import Catalog
import zstandard as zstd
import json
from pydantic import BaseModel
from datetime import date
from .conversion import models_to_dataframe

HTMLFile = str
JSONFile = dict
Data = list[BaseModel]
DataWithRunId = list[tuple[str, BaseModel]]
DataWithRunIdInfo = list[tuple[str, RunInfo, BaseModel]]

RawFile = HTMLFile | JSONFile
RawData = list[RawFile]
RawDataWithRunIdAndInfo = list[tuple[str, RunInfo, RawFile, str]]


class Parser:
    def __init__(
        self,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        handler: Callable[[RawFile, RunInfo | None], Data],
        input_storage: Storage,
        output_storage: Storage,
        run_info: RunInfo,
    ):
        self.bronze_catalog = Catalog(
            input_storage, output_storage, project_name, scraper_name, "bronze"
        )

        self.bronze = BronzeLayer(
            input_storage,
            project_name,
            scraper_name,
            run_id,
            identifier,
            self.bronze_catalog,
        )
        self.silver = SilverLayer(output_storage, project_name, scraper_name, run_id)
        self.handler = handler
        self.run_info = run_info

    def detect_file_type(self):
        signature = inspect.signature(self.handler)
        params = [p for p in signature.parameters.values()]
        first_param = params[0]

        hints = get_type_hints(self.handler)
        hint = hints.get(first_param.name, first_param.annotation)
        if hint is HTMLFile or hint is JSONFile:
            return hint
        raise ValueError("Unsupported handler input type")

    async def _get_run_info_files(self, key: str) -> dict[str, RunInfo]:
        run_info_file_names = await self.bronze_catalog.list_files(key, "*run.json")
        run_info_files = [
            file async for file in self.bronze.storage.load_files(run_info_file_names)
        ]
        run_infos = {
            file.name.split("run=")[1].split("/")[0]: RunInfo(
                **json.loads(file.content.decode("utf-8"))
            )
            for file in run_info_files
        }
        return run_infos

    async def load_input_files(
        self, key: str, run_infos: dict[str, RunInfo]
    ) -> AsyncIterator[tuple[str, RunInfo, RawFile, str]]:
        input_type = self.detect_file_type()
        file_names = await self.bronze_catalog.list_files(key, "*.zst")
        files = self.bronze.storage.load_files(file_names)
        dctx = zstd.ZstdDecompressor()
        async for file in files:
            decompressed_file = dctx.decompress(file.content)
            run_id = file.name.split("run=")[1].split("/")[0]
            if input_type is HTMLFile:
                yield (
                    run_id,
                    run_infos[run_id],
                    decompressed_file.decode("utf-8"),
                    file.name,
                )
            elif input_type is JSONFile:
                yield (
                    run_id,
                    run_infos[run_id],
                    json.loads(decompressed_file.decode("utf-8")),
                    file.name,
                )
            else:
                raise ValueError("Unsupported handler input type")

    async def load_run_input_files(
        self,
    ) -> AsyncIterator[tuple[str, RunInfo, RawFile, str]]:
        run_infos = await self._get_run_info_files(self.bronze._run_path)
        async for file in self.load_input_files(self.bronze.files_path, run_infos):
            yield file

    async def parse(self, raw_data: RawDataWithRunIdAndInfo) -> DataWithRunIdInfo:
        results = []

        # Inspect handler once
        sig = inspect.signature(self.handler)
        type_hints = get_type_hints(self.handler)
        allowed_param_names = sig.parameters.keys()
        # Iterate and call handler with the same kwargs
        for run_id, run_info, raw_file, name in raw_data:
            # Prepare kwargs only if handler expects a RunInfo
            context = {
                "run_info": run_info,
                "file_name": name,
            }
            kwargs = {
                name: value
                for name, value in context.items()
                if name in allowed_param_names
            }
            parsed_data = self.handler(raw_file, **kwargs)
            results.extend([(run_id, run_info, pdt) for pdt in parsed_data])

        return results

    async def save(
        self, data: DataWithRunIdInfo, batch_size: int = 10000, outer_index: int = 0
    ) -> int:
        if not data:
            return outer_index
        number_of_rows = len(data)
        number_of_batches = math.ceil(number_of_rows / batch_size)
        for i in range(number_of_batches):
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = data[start_index:end_index]
            models = [item for _, _, item in batch_data]
            df = models_to_dataframe(models)
            df["source_run_id"] = [source_run_id for source_run_id, _, _ in batch_data]
            df["scraped_at"] = [run_info.requested_at for _, run_info, _ in batch_data]
            df["parsed_at"] = [self.run_info.requested_at for _, _, _ in batch_data]
            buf = BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")  # type: ignore[arg-type]
            file_name = f"{generate_run_id()}-{i + outer_index:06d}.parquet"
            await self.silver.save(file_name, buf.getvalue())
        return number_of_batches + outer_index

    @classmethod
    async def run(
        cls,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        handler: Callable[[RawFile, RunInfo | None], Data],
        run_info: RunInfo,
        input_storage: Storage,
        output_storage: Storage = None,
        full_run: bool = False,
        date_filter: date = None,
    ):
        parser = cls(
            project_name,
            scraper_name,
            run_id,
            identifier,
            handler,
            input_storage,
            output_storage or input_storage,
            run_info,
        )
        await parser.silver.mark_run_as_started()
        await parser.silver.create_run_info(run_info)
        if full_run:
            scraper_name = BronzeLayer._create_scraper_path(project_name, scraper_name)
            run_infos = await parser._get_run_info_files(scraper_name)
            raw_data = parser.load_input_files(scraper_name, run_infos)
        elif date_filter:
            scraper_name = f"{BronzeLayer._create_scraper_path(project_name, scraper_name)}/{date_filter.strftime('%Y/%m/%d')}"
            run_infos = await parser._get_run_info_files(scraper_name)
            raw_data = parser.load_input_files(scraper_name, run_infos)
        else:
            raw_data = parser.load_run_input_files()

        try:
            empty = True
            parsed_data = []
            async for raw_d in raw_data:
                empty = False
                parser_d = await parser.parse([raw_d])
                parsed_data.extend(parser_d)

            if empty:
                raise ValueError("No input files found to parse.")

            await parser.save(parsed_data)
            await parser.silver.mark_run_as_completed()

        except Exception as e:
            await parser.silver.mark_run_as_failed()
            raise e

        finally:
            await parser.silver.storage.close()
            await parser.bronze.storage.close()
