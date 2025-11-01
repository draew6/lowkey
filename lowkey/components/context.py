from typing import Literal, TypeVar, Generic
from crawlee.crawlers import (
    ParsedHttpCrawlingContext as OldParsedHttpCrawlingContext,
    BeautifulSoupCrawlingContext as OldBeautifulSoupCrawlingContext,
    BasicCrawlingContext as OldBasicCrawlingContext,
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


class ParsedHttpCrawlingContext(
    _DiscoveryContextMixin, OldParsedHttpCrawlingContext, Generic[TParseResult]
):
    session: Session


class BeautifulSoupCrawlingContext(
    _DiscoveryContextMixin, OldBeautifulSoupCrawlingContext
):
    session: Session


class BasicCrawlingContext(_DiscoveryContextMixin, OldBasicCrawlingContext):
    session: Session
