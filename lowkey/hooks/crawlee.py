import json
from typing import Callable
from crawlee.crawlers import ParsedHttpCrawlingContext
from .decorators import after_handler
from ..storage import ScraperStorage
import zstandard as zstd
import random
from ..utils import extract_cookies


@after_handler
async def save_raw_html(
    storage: ScraperStorage,
    identifier_value_fn: Callable[[str], str],
    context: ParsedHttpCrawlingContext,
):
    """Saves the raw HTML response to storage."""
    url = context.request.url
    identifier_value = identifier_value_fn(url)
    body = await context.http_response.read()
    cctx = zstd.ZstdCompressor(level=10)  # TODO: možno potestovať čo sa oplati
    compressed_body = cctx.compress(body)
    await storage.bronze.save("response.body.zst", identifier_value, compressed_body)


@after_handler
async def save_request_crawlee_metadata(
    storage: ScraperStorage,
    identifier_value_fn: Callable[[str], str],
    context: ParsedHttpCrawlingContext,
):
    request_dict = context.request.model_dump()
    session_dict = context.session.get_state(as_dict=True)
    file_dict = {
        "request": request_dict,
        "session": session_dict,
    }
    file = json.dumps(file_dict, default=str).encode("utf-8")
    identifier_value = identifier_value_fn(context.request.url)
    await storage.bronze.save("request.crawlee.json", identifier_value, file)


@after_handler
async def save_response_crawlee_metadata(
    storage: ScraperStorage,
    identifier_value_fn: Callable[[str], str],
    context: ParsedHttpCrawlingContext,
):
    response = context.http_response
    request = context.request
    response_meta = {
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "url": request.url,
        "http_version": response.http_version,
    }
    file = json.dumps(response_meta).encode("utf-8")
    identifier_value = identifier_value_fn(context.request.url)
    await storage.bronze.save("response.crawlee.json", identifier_value, file)


@after_handler
async def random_heartbeat(
    storage: ScraperStorage,
    probability: float,
    context: ParsedHttpCrawlingContext,
):
    should_heartbeat = random.random() < probability
    if should_heartbeat:
        await storage.bronze.heartbeat()


@after_handler
async def save_cookies(
    context: ParsedHttpCrawlingContext,
):
    cookies = extract_cookies(context.http_response.headers)
    if cookies:
        context.session.user_data["cookies"] = {
            **context.session.user_data.get("cookies", {}),
            **cookies,
        }

@after_handler
async def save_user_info(
    storage: ScraperStorage,
    identifier_value_fn: Callable[[str], str],
    context: ParsedHttpCrawlingContext,
):
    proxy = context.session.user_data.get("proxy_url")
    user = {
        "proxy": proxy,
    }
    user_file = json.dumps(user).encode("utf-8")
    identifier_value = identifier_value_fn(context.request.url)
    await storage.bronze.save("user.json", identifier_value, user_file)