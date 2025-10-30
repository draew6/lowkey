from typing import Unpack, AsyncGenerator
from bs4 import BeautifulSoup, Tag
from crawlee.crawlers import BeautifulSoupParserType, BasicCrawlerOptions, AbstractHttpCrawler
from .context import BeautifulSoupCrawlingContext, ParsedHttpCrawlingContext
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser


class BeautifulSoupCrawler(AbstractHttpCrawler[BeautifulSoupCrawlingContext, BeautifulSoup, Tag]):

    def __init__(
            self,
            *,
            parser: BeautifulSoupParserType = 'lxml',
            **kwargs: Unpack[BasicCrawlerOptions[BeautifulSoupCrawlingContext]],
    ) -> None:
        """Initialize a new instance.

        Args:
            parser: The type of parser that should be used by `BeautifulSoup`.
            kwargs: Additional keyword arguments to pass to the underlying `AbstractHttpCrawler`.
        """

        async def final_step(
                context: ParsedHttpCrawlingContext[BeautifulSoup],
        ) -> AsyncGenerator[BeautifulSoupCrawlingContext, None]:
            """Enhance `ParsedHttpCrawlingContext[BeautifulSoup]` with `soup` property."""
            yield BeautifulSoupCrawlingContext.from_parsed_http_crawling_context(context)

        kwargs['_context_pipeline'] = self._create_static_content_crawler_pipeline().compose(final_step)

        super().__init__(
            parser=BeautifulSoupParser(parser=parser),
            **kwargs,
        )