import json
from typing import Callable
import httpx
from curlify2 import Curlify
from ..storage import ScraperStorage


def make_save_request_curl(
    storage: ScraperStorage, identifier_value_fn: Callable[[str], str | None]
):
    async def save_request_curl(request: httpx.Request):
        phase = request.extensions.get("lk-phase")
        work_type = request.extensions.get("lk-work-type")
        if work_type == "BEFORE_START":
            return None
        identifier_value = identifier_value_fn(str(request.url))
        curl_request = Curlify(request)
        curl = curl_request.to_curl()
        curl_file = curl.encode("utf-8")
        request_meta = {
            "method": request.method,
            "url": str(request.url),
            "headers": [
                (k.decode("latin-1"), v.decode("latin-1"))
                for k, v in request.headers.raw
            ],
        }
        await storage.bronze.save("request.curl", identifier_value, curl_file)
        await storage.bronze.save(
            "request.meta.json",
            identifier_value,
            json.dumps(request_meta, default=str).encode("utf-8"),
        )
        return None

    return save_request_curl
