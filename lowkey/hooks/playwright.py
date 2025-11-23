from crawlee.crawlers import PlaywrightCrawlingContext
from playwright.async_api import Response, Request
from .decorators.before_handler import before_handler
import json
from typing import Callable
from ..storage import ScraperStorage
import random


@before_handler
async def random_heartbeat(
    storage: ScraperStorage,
    probability: float,
    request_filter: Callable[[Request | Response], bool],
    context: PlaywrightCrawlingContext,
):
    async def hook(request: Request):
        if not (request_filter(request)):
            return
        should_heartbeat = random.random() < probability
        if should_heartbeat:
            await storage.bronze.heartbeat()

    context.page.on("request", hook)


@before_handler
async def save_request_crawlee_metadata(
    storage: ScraperStorage,
    identifier_value_fn: Callable[[Request], str],
    request_filter: Callable[[Request | Response], bool],
    context: PlaywrightCrawlingContext,
):
    async def hook(request: Request):
        if not (request_filter(request)):
            return
        request_dict = context.request.model_dump()
        session_dict = context.session.get_state(as_dict=True)

        file_dict = {
            "request": request_dict,
            "session": session_dict,
        }

        file = json.dumps(file_dict, default=str).encode("utf-8")
        try:
            identifier_value = identifier_value_fn(request)
        except Exception:
            return
        await storage.bronze.save("request.crawlee.json", identifier_value, file)

    context.page.on("request", hook)


@before_handler
async def save_response_crawlee_metadata(
    storage: ScraperStorage,
    identifier_value_fn: Callable[[Request], str],
    request_filter: Callable[[Request | Response], bool],
    context: PlaywrightCrawlingContext,
):
    async def hook(response: Response):
        if not (request_filter(response)):
            return
        response_meta = {
            "status_code": response.status,
            "headers": dict(response.headers),
            "url": response.request.url,
        }
        file = json.dumps(response_meta).encode("utf-8")
        try:
            identifier_value = identifier_value_fn(response.request)
        except Exception:
            return
        await storage.bronze.save("response.crawlee.json", identifier_value, file)

    context.page.on("response", hook)
