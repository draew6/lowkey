from datetime import timedelta
from typing import Callable
from crawlee.router import Router
from crawlee import ConcurrencySettings
from crawlee.storages import RequestQueue
from .components.session import SessionPool
from .components.proxyconfig import ProxyConfiguration
from .components.crawler import PlaywrightCrawler
from .components.crawler import BeautifulSoupCrawler
from .hooks.httpx import make_save_request_curl
from .models.client import APIClient
from .storage import ScraperStorage, Storage
from .components.httpclient import HttpxHttpClient
from .models.user import User
from .components.playwright import PlaywrightBrowserPlugin, BrowserPool
from .components.context import BeautifulSoupCrawlingContext
from .utils import random_sleep


async def create_crawler(
    project_name: str,
    scraper_name: str,
    run_id: str,
    identifier: str,
    users: list[User],
    storage: Storage,
    api_client: APIClient,
    identifier_value_fn=Callable[[str], str | None],
    save_request: bool = True,
    wait_time_between_requests: float = 3,
    regen_time: int = 3,
    is_browser: bool = False,
    debug: bool = False,
    follow_redirects: bool = True,
) -> tuple[BeautifulSoupCrawler, ScraperStorage, Router[BeautifulSoupCrawlingContext]]:
    session_pool = SessionPool(
        users=users, persistence_enabled=False, regen_time=regen_time
    )
    proxy_configuration = ProxyConfiguration(users=users)
    scraper_storage = ScraperStorage(
        storage, project_name, scraper_name, run_id, identifier, api_client
    )
    http_client = HttpxHttpClient.get_client_with_hooks(
        [make_save_request_curl(scraper_storage, identifier_value_fn)]
        if save_request
        else None, follow_redirects=follow_redirects
    )
    request_manager = await RequestQueue.open(name=run_id)

    router = Router[BeautifulSoupCrawlingContext]()

    if is_browser:
        plugin = PlaywrightBrowserPlugin.with_user_fingerprints(
            users, headless=not debug
        )
        crawler = PlaywrightCrawler(
            browser_pool=BrowserPool(plugins=[plugin]),
            request_handler=router,
            session_pool=session_pool,
            proxy_configuration=proxy_configuration,
            concurrency_settings=ConcurrencySettings(
                max_tasks_per_minute=60, desired_concurrency=1, max_concurrency=2
            ),
            request_manager=request_manager,
            request_handler_timeout=timedelta(minutes=15),
        )
    else:
        crawler = BeautifulSoupCrawler(
            request_handler=router,
            session_pool=session_pool,
            proxy_configuration=proxy_configuration,
            concurrency_settings=ConcurrencySettings(
                max_tasks_per_minute=60, desired_concurrency=1, max_concurrency=2
            ),
            http_client=http_client,
            request_manager=request_manager,
        )

    @crawler.pre_navigation_hook
    async def apply_user_agent(context: BeautifulSoupCrawlingContext):
        ua = context.session.user_data["user_agent"]
        context.log.debug(f"Applying UA for {context.session.id}: {ua}")
        context.request.headers = context.request.headers | {"User-Agent": ua}

    @crawler.pre_navigation_hook
    async def apply_cookies(context: BeautifulSoupCrawlingContext):
        cookies = context.session.user_data.get("cookies")
        if cookies:
            context.log.debug(f"Applying cookies for {context.session.id}: {cookies}")
            cookie_header = "; ".join(
                [f"{cookie['name']}={cookie['value']}" for cookie in cookies]
            )
            context.request.headers = context.request.headers | {
                "Cookie": cookie_header
            }

    @crawler.pre_navigation_hook
    async def pass_user_phase_for_httpx(context: BeautifulSoupCrawlingContext):
        work_type = context.request.user_data.get("work_type", context.session.phase)
        phase = "DISCOVERY" if work_type == "BEFORE_START" else context.session.phase
        context.request.headers = context.request.headers | {
            "lk-phase": phase,
            "lk-work-type": work_type,
        }

    @crawler.pre_navigation_hook
    async def wait_between_requests(context: BeautifulSoupCrawlingContext):
        wait_time = max(regen_time, int(wait_time_between_requests))
        await random_sleep(wait_time)

    @crawler.router.handler("visit")
    async def visit_handler(context: BeautifulSoupCrawlingContext): ...

    return crawler, scraper_storage, router
