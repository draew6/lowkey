from typing import Literal, TypeVar, Generic
from crawlee.crawlers import ParsedHttpCrawlingContext as OldParsedHttpCrawlingContext
from .session import Session
from bs4 import BeautifulSoup

TParseResult = TypeVar('TParseResult')

class ParsedHttpCrawlingContext(OldParsedHttpCrawlingContext, Generic[TParseResult]):
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


class BeautifulSoupCrawlingContext(ParsedHttpCrawlingContext[BeautifulSoup]): ...
