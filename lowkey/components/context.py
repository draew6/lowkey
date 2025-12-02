from typing import Literal, TypeVar, Generic
from crawlee.crawlers import (
    ParsedHttpCrawlingContext as OldParsedHttpCrawlingContext,
    BeautifulSoupCrawlingContext as OldBeautifulSoupCrawlingContext,
    BasicCrawlingContext as OldBasicCrawlingContext,
    PlaywrightCrawlingContext as OldPlaywrightCrawlingContext,
)
from .session import Session

TParseResult = TypeVar("TParseResult")


class _DiscoveryContextMixin:
    session: Session

    def continue_discovery(self) -> None:
        self.session.turn_to_discovery_phase()

    def finalize_discovery(self) -> None:
        self.session.turn_to_final_phase()

    @property
    def is_in_discovery_phase(self) -> bool:
        return self.session.is_in_discovery_phase()

    @property
    def phase(self) -> Literal["DISCOVERY", "FINAL"]:
        return self.session.phase


class _ActionContextMixin:
    session: Session

    def ignore_request(self) -> None:
        self.session.ignore_request()

    def process_request(self) -> None:
        self.session.process_request()

    @property
    def action(self) -> Literal["IGNORE", "PROCESS"]:
        return self.action

    @property
    def should_ignore(self) -> bool:
        return self.session.should_ignore


class ParsedHttpCrawlingContext(
    _DiscoveryContextMixin,
    _ActionContextMixin,
    OldParsedHttpCrawlingContext,
    Generic[TParseResult],
):
    session: Session


class BeautifulSoupCrawlingContext(
    _DiscoveryContextMixin, _ActionContextMixin, OldBeautifulSoupCrawlingContext
):
    session: Session


class BasicCrawlingContext(
    _DiscoveryContextMixin, _ActionContextMixin, OldBasicCrawlingContext
):
    session: Session


class PlaywrightCrawlingContext(OldPlaywrightCrawlingContext):
    async def spa_navigate(self, url: str) -> None:
        """Navigates to the specified URL in a Single Page Application (SPA) context.

        This method uses client-side navigation to change the URL without reloading the entire page.
        It is particularly useful for SPAs where traditional navigation methods may not work as expected.

        Args:
            url (str): The URL to navigate to.
        """
        await self.page.evaluate(
            """(url) => {
                history.pushState({}, "", url);
                window.dispatchEvent(new Event('popstate'));
            }""",
            url,
        )
