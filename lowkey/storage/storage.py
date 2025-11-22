from crawlee.crawlers import BeautifulSoupCrawler, PlaywrightCrawler
from ..models.client import APIClient
from ..models.user import User
from .client import Storage
from .info import ScraperInfo, RunInfo
from .layer import BronzeLayer, SilverLayer
from .catalog import Catalog


class ScraperStorage:
    def __init__(
        self,
        storage: Storage,
        project_name: str,
        scraper_name: str,
        run_id: str,
        identifier: str,
        api_client: APIClient,
    ) -> None:
        bronze_catalog = Catalog(storage, storage, project_name, scraper_name, "bronze")
        self.bronze = BronzeLayer(
            storage, project_name, scraper_name, run_id, identifier, bronze_catalog
        )
        self.silver = SilverLayer(storage, project_name, scraper_name, run_id)
        self.api_client = api_client

    async def start_run(
        self,
        crawler: BeautifulSoupCrawler | PlaywrightCrawler,
        run_info: RunInfo,
        actor_info: ScraperInfo,
    ):
        await self.bronze.create_run_info(run_info)
        await self.bronze.create_actor_info(actor_info)
        await self.bronze.mark_run_as_started()
        await crawler.run()
        await self.bronze.mark_run_as_completed()

        await self.bronze.storage.close()
        await self.silver.storage.close()
        # TODO: ak session je vylučena z poolu, tak dostanem ju tu aby som ju mohol updatnut?
        users = crawler._session_pool.create_users_from_sessions()
        await User.update_users(self.api_client, users)
        await crawler._request_manager.drop()

    @staticmethod
    async def start_run_for_multiple(
        storages: list["ScraperStorage"],
        crawler: BeautifulSoupCrawler | PlaywrightCrawler,
        run_info: RunInfo,
        actor_info: ScraperInfo,
    ):
        for storage in storages:
            await storage.bronze.create_run_info(run_info)
            await storage.bronze.create_actor_info(actor_info)
            await storage.bronze.mark_run_as_started()
        await crawler.run()
        for storage in storages:
            await storage.bronze.mark_run_as_completed()
            await storage.bronze.storage.close()
            await storage.silver.storage.close()