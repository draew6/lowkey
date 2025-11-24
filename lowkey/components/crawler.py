import asyncio
import traceback
import warnings
from functools import partial
from typing import Unpack, AsyncGenerator, cast
from bs4 import BeautifulSoup, Tag
from crawlee._request import RequestState
from crawlee._types import RequestHandlerRunResult
from crawlee._utils.wait import wait_for
from crawlee.crawlers import (
    BeautifulSoupParserType,
    BasicCrawlerOptions,
    AbstractHttpCrawler,
    PlaywrightPreNavCrawlingContext,
)
from crawlee.crawlers._playwright._playwright_http_client import browser_page_context
from crawlee.crawlers._playwright._utils import infinite_scroll, block_requests
from crawlee.errors import (
    ContextPipelineInitializationError,
    ContextPipelineInterruptedError,
    SessionError,
    RequestHandlerError,
    RequestCollisionError,
)

from .context import (
    BeautifulSoupCrawlingContext,
    ParsedHttpCrawlingContext,
    BasicCrawlingContext,
    PlaywrightCrawlingContext,
)
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser
from crawlee.crawlers._playwright._playwright_crawler import (
    PlaywrightCrawler as OldPlaywrightCrawler,
)


class BeautifulSoupCrawler(
    AbstractHttpCrawler[BeautifulSoupCrawlingContext, BeautifulSoup, Tag]
):
    def __init__(
        self,
        *,
        parser: BeautifulSoupParserType = "lxml",
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
            yield BeautifulSoupCrawlingContext.from_parsed_http_crawling_context(
                context
            )

        kwargs["_context_pipeline"] = (
            self._create_static_content_crawler_pipeline().compose(final_step)
        )

        super().__init__(
            parser=BeautifulSoupParser(parser=parser),
            **kwargs,
        )

    async def __run_task_function(self) -> None:
        request_manager = await self.get_request_manager()

        request = await wait_for(
            lambda: request_manager.fetch_next_request(),
            timeout=self._internal_timeout,
            timeout_message=f"Fetching next request failed after {self._internal_timeout.total_seconds()} seconds",
            logger=self._logger,
            max_retries=3,
        )

        if request is None:
            return

        if not (await self._is_allowed_based_on_robots_txt_file(request.url)):
            self._logger.warning(
                f"Skipping request {request.url} ({request.unique_key}) because it is disallowed based on robots.txt"
            )

            await self._handle_skipped_request(request, "robots_txt", need_mark=True)
            return

        if request.session_id:
            session = await self._get_session_by_id(request.session_id)
        else:
            session = await self._get_session()
        proxy_info = await self._get_proxy_info(request, session)
        result = RequestHandlerRunResult(
            key_value_store_getter=self.get_key_value_store
        )

        context = BasicCrawlingContext(
            request=request,
            session=session,
            proxy_info=proxy_info,
            send_request=self._prepare_send_request_function(session, proxy_info),
            add_requests=result.add_requests,
            push_data=result.push_data,
            get_key_value_store=result.get_key_value_store,
            use_state=self._use_state,
            log=self._logger,
        )
        self._context_result_map[context] = result

        self._statistics.record_request_processing_start(request.unique_key)

        try:
            request.state = RequestState.REQUEST_HANDLER

            self._check_request_collision(context.request, context.session)

            try:
                await self._run_request_handler(context=context)
            except asyncio.TimeoutError as e:
                raise RequestHandlerError(e, context) from e

            await self._commit_request_handler_result(context)
            await wait_for(
                lambda: request_manager.mark_request_as_handled(context.request),
                timeout=self._internal_timeout,
                timeout_message="Marking request as handled timed out after "
                f"{self._internal_timeout.total_seconds()} seconds",
                logger=self._logger,
                max_retries=3,
            )

            request.state = RequestState.DONE

            if context.session and context.session.is_usable:
                context.session.mark_good()

            self._statistics.record_request_processing_finish(request.unique_key)

        except RequestCollisionError as request_error:
            context.request.no_retry = True
            await self._handle_request_error(context, request_error)

        except RequestHandlerError as primary_error:
            primary_error = cast(
                "RequestHandlerError[TCrawlingContext]", primary_error
            )  # valid thanks to ContextPipeline

            self._logger.debug(
                "An exception occurred in the user-defined request handler",
                exc_info=primary_error.wrapped_exception,
            )
            await self._handle_request_error(
                primary_error.crawling_context, primary_error.wrapped_exception
            )

        except SessionError as session_error:
            if not context.session:
                raise RuntimeError(
                    "SessionError raised in a crawling context without a session"
                ) from session_error

            if self._error_handler:
                await self._error_handler(context, session_error)

            if self._should_retry_request(context, session_error):
                exc_only = "".join(
                    traceback.format_exception_only(session_error)
                ).strip()
                self._logger.warning(
                    'Encountered "%s", rotating session and retrying...', exc_only
                )

                context.session.retire()

                # Increment session rotation count.
                context.request.session_rotation_count = (
                    context.request.session_rotation_count or 0
                ) + 1

                await request_manager.reclaim_request(request)
                await self._statistics.error_tracker_retry.add(
                    error=session_error, context=context
                )
            else:
                await wait_for(
                    lambda: request_manager.mark_request_as_handled(context.request),
                    timeout=self._internal_timeout,
                    timeout_message="Marking request as handled timed out after "
                    f"{self._internal_timeout.total_seconds()} seconds",
                    logger=self._logger,
                    max_retries=3,
                )

                await self._handle_failed_request(context, session_error)
                self._statistics.record_request_processing_failure(request.unique_key)

        except ContextPipelineInterruptedError as interrupted_error:
            self._logger.debug(
                "The context pipeline was interrupted", exc_info=interrupted_error
            )

            await wait_for(
                lambda: request_manager.mark_request_as_handled(context.request),
                timeout=self._internal_timeout,
                timeout_message="Marking request as handled timed out after "
                f"{self._internal_timeout.total_seconds()} seconds",
                logger=self._logger,
                max_retries=3,
            )

        except ContextPipelineInitializationError as initialization_error:
            self._logger.debug(
                "An exception occurred during the initialization of crawling context",
                exc_info=initialization_error,
            )
            await self._handle_request_error(
                context, initialization_error.wrapped_exception
            )

        except Exception as internal_error:
            self._logger.exception(
                "An exception occurred during handling of a request. This places the crawler "
                "and its underlying storages into an unknown state and crawling will be terminated.",
                exc_info=internal_error,
            )
            raise


class PlaywrightCrawler(OldPlaywrightCrawler):
    async def _navigate(
        self,
        context: PlaywrightPreNavCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, Exception | None]:
        """Execute an HTTP request utilizing the `BrowserPool` and the `Playwright` library.

        Args:
            context: The basic crawling context to be enhanced.

        Raises:
            ValueError: If the browser pool is not initialized.
            SessionError: If the URL cannot be loaded by the browser.

        Yields:
            The enhanced crawling context with the Playwright-specific features (page, response, enqueue_links,
                infinite_scroll and block_requests).
        """
        async with context.page:
            if context.session:
                session_cookies = (
                    context.session.cookies.get_cookies_as_playwright_format()
                )
                await self._update_cookies(context.page, session_cookies)

            if context.request.headers:
                await context.page.set_extra_http_headers(
                    context.request.headers.model_dump()
                )
            # Navigate to the URL and get response.
            if context.request.method != "GET":
                # Call the notification only once
                warnings.warn(
                    "Using other request methods than GET or adding payloads has a high impact on performance"
                    " in recent versions of Playwright. Use only when necessary.",
                    category=UserWarning,
                    stacklevel=2,
                )

                route_handler = self._prepare_request_interceptor(
                    method=context.request.method,
                    headers=context.request.headers,
                    payload=context.request.payload,
                )

                # Set route_handler only for current request
                await context.page.route(context.request.url, route_handler)

            response = await context.page.goto(context.request.url)

            if response is None:
                raise SessionError(f"Failed to load the URL: {context.request.url}")

            # Set the loaded URL to the actual URL after redirection.
            context.request.loaded_url = context.page.url

            extract_links = self._create_extract_links_function(context)

            async with browser_page_context(context.page):
                error = yield PlaywrightCrawlingContext(
                    request=context.request,
                    session=context.session,
                    add_requests=context.add_requests,
                    send_request=context.send_request,
                    push_data=context.push_data,
                    use_state=context.use_state,
                    proxy_info=context.proxy_info,
                    get_key_value_store=context.get_key_value_store,
                    log=context.log,
                    page=context.page,
                    infinite_scroll=lambda: infinite_scroll(context.page),
                    response=response,
                    extract_links=extract_links,
                    enqueue_links=self._create_enqueue_links_function(
                        context, extract_links
                    ),
                    block_requests=partial(block_requests, page=context.page),
                )

            if context.session:
                pw_cookies = await self._get_cookies(context.page)
                context.session.cookies.set_cookies_from_playwright_format(pw_cookies)

            # Collect data in case of errors, before the page object is closed.
            if error:
                await self.statistics.error_tracker.add(
                    error=error, context=context, early=True
                )
