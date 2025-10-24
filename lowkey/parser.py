import inspect
import math
from io import BytesIO
from typing import Callable, get_type_hints
from lowkey import RunInfo
from lowkey.storage import SilverLayer, Storage
from lowkey.storage import BronzeLayer
import zstandard as zstd
import json
import pandas as pd
from pydantic import BaseModel


HTMLFile = str
JSONFile = dict
Data = list[BaseModel]

RawFile = HTMLFile | JSONFile
RawData = list[RawFile]


class Parser:
    def __init__(
        self,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        handler: Callable[[RawFile], Data],
        input_storage: Storage,
        output_storage: Storage,
    ):
        self.bronze = BronzeLayer(
            input_storage, project_name, scraper_name, run_id, identifier
        )
        self.silver = SilverLayer(output_storage, project_name, scraper_name, run_id)
        self.handler = handler

    def detect_file_type(self):
        signature = inspect.signature(self.handler)
        params = [p for p in signature.parameters.values()]
        first_param = params[0]

        hints = get_type_hints(self.handler)
        hint = hints.get(first_param.name, first_param.annotation)
        if hint is HTMLFile or hint is JSONFile:
            return hint
        raise ValueError("Unsupported handler input type")

    async def load_input_files(self) -> RawData:
        input_type = self.detect_file_type()
        files = await self.bronze.load_run_files("*.zst")
        dctx = zstd.ZstdDecompressor()
        decompressed_files = [dctx.decompress(file.content) for file in files]
        if input_type is HTMLFile:
            return [file.decode("utf-8") for file in decompressed_files]
        elif input_type is JSONFile:
            return [json.loads(file.decode("utf-8")) for file in decompressed_files]
        else:
            raise ValueError("Unsupported handler input type")

    async def parse(self, raw_data: RawData) -> Data:
        results = []
        for raw_file in raw_data:
            parsed_data = self.handler(raw_file)
            results.extend(parsed_data)
        return results

    async def save(self, data: Data, batch_size: int = 10000):
        if not data:
            return
        number_of_rows = len(data)
        number_of_batches = math.ceil(number_of_rows / batch_size)
        for i in range(number_of_batches):
            start_index = i * batch_size
            end_index = start_index + batch_size
            batch_data = data[start_index:end_index]
            rows = [item.model_dump() for item in batch_data]
            df = pd.DataFrame(rows)
            buf = BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")  # type: ignore[arg-type]
            file_name = f"{i:06d}.parquet"
            await self.silver.save(file_name, buf.getvalue())

    @classmethod
    async def run(
        cls,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        handler: Callable[[RawFile], Data],
        run_info: RunInfo,
        input_storage: Storage,
        output_storage: Storage = None,
    ):
        parser = cls(
            project_name,
            scraper_name,
            run_id,
            identifier,
            handler,
            input_storage,
            output_storage or input_storage,
        )
        await parser.silver.mark_run_as_started()
        await parser.silver.create_run_info(run_info)
        raw_data = await parser.load_input_files()
        try:
            parsed_data = await parser.parse(raw_data)
            await parser.save(parsed_data)
        except Exception as e:
            await parser.silver.mark_run_as_failed()
            raise e
        await parser.silver.mark_run_as_completed()
        await parser.bronze.storage.close()
        await parser.silver.storage.close()
