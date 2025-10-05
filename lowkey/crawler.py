from typing import Callable
from crawlee.router import Router
from crawlee import ConcurrencySettings
from crawlee.storages import RequestQueue
from .components.sessionpool import SessionPool
from .components.proxyconfig import ProxyConfiguration
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from .hooks.httpx import make_save_request_curl
from .models.client import APIClient
from .storage import ScraperStorage, Storage
from .components.httpclient import HttpxHttpClient
from .models.user import User


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
) -> tuple[BeautifulSoupCrawler, ScraperStorage, Router[BeautifulSoupCrawlingContext]]:
    session_pool = SessionPool(users=users, persistence_enabled=False, regen_time=2)
    proxy_configuration = ProxyConfiguration(users=users)
    scraper_storage = ScraperStorage(
        storage, project_name, scraper_name, run_id, identifier, api_client
    )
    http_client = HttpxHttpClient.get_client_with_hooks(
        [make_save_request_curl(scraper_storage, identifier_value_fn)]
        if save_request
        else None
    )
    request_manager = await RequestQueue.open(name=run_id)

    router = Router[BeautifulSoupCrawlingContext]()

    crawler = BeautifulSoupCrawler(
        request_handler=router,
        session_pool=session_pool,
        proxy_configuration=proxy_configuration,
        concurrency_settings=ConcurrencySettings(max_tasks_per_minute=1000),
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
                [f"{key}={value}" for key, value in cookies.items()]
            )
            context.request.headers = context.request.headers | {
                "Cookie": cookie_header
            }

    @crawler.router.handler("visit")
    async def visit_handler(context: BeautifulSoupCrawlingContext): ...

    return crawler, scraper_storage, router
