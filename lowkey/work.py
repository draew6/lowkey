import json
import random
from dataclasses import dataclass
from typing import Callable
from crawlee import Request
from crawlee._types import HttpMethod
from crawlee.crawlers import (
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)
from crawlee.router import Router
from .crawler import create_crawler
from .models.client import APIClient
from .storage import Storage, ScraperStorage
from .models.user import User


@dataclass
class WorkUnit:
    url: str
    method: HttpMethod
    payload: dict | None = None


def create_requests(
    work: list[WorkUnit],
    before_start_urls: list[str],
    users: list[User],
    handler_name: str | None,
):
    requests = []
    for user in users:
        for url in before_start_urls:
            request = Request.from_url(
                url=url,
                method="GET",
                label="visit",
                session_id=user.session_id,
                unique_key=f"visit_{user.session_id}{random.randint(0, 10000)}",
                payload=None,
            )
            request.user_data["work_type"] = "BEFORE_START"
            requests.append(request)

    for i, work_unit in enumerate(work):
        user = users[i % len(users)]
        request = Request.from_url(
            url=work_unit.url,
            method=work_unit.method,
            label=handler_name,
            session_id=user.session_id,
            unique_key=None,
            payload=json.dumps(work_unit.payload) if work_unit.payload else None,
        )
        request.user_data["work_type"] = "WORK"
        requests.append(request)

    return requests


async def get_crawler(
    project_name: str,
    scraper_name: str,
    run_id: str,
    identifier: str,
    users: list[User],
    storage: Storage,
    api_client: APIClient,
    work: list[WorkUnit],
    before_start_urls: list[str],
    identifier_value_fn=Callable[[str], str | None],
    handler_name: str | None = None,
    save_request: bool = True,
    wait_time_between_requests: float = 3.0,
    regen_time: int = 3,
    is_browser: bool = False,
    debug: bool = False,
) -> tuple[BeautifulSoupCrawler, ScraperStorage, Router[BeautifulSoupCrawlingContext]]:
    crawler, scraper_storage, router = await create_crawler(
        project_name,
        scraper_name,
        run_id,
        identifier,
        users,
        storage,
        api_client,
        identifier_value_fn,
        save_request,
        wait_time_between_requests,
        regen_time,
        is_browser,
        debug,
    )
    requests = create_requests(work, before_start_urls, users, handler_name)
    await crawler.add_requests(requests=requests)
    return crawler, scraper_storage, router
