from collections import defaultdict
from typing import Mapping, Any, override
from browserforge.fingerprints import (
    Fingerprint,
    ScreenFingerprint,
    NavigatorFingerprint,
    VideoCard,
)
from browserforge.injectors.playwright import AsyncNewContext
from crawlee.browsers import (
    PlaywrightBrowserPlugin as OldPlaywrightBrowserPlugin,
    PlaywrightBrowserController as OldPlaywrightBrowserController,
    PlaywrightPersistentBrowser,
    BrowserPool as OldBrowserPool,
)
from crawlee.browsers._types import BrowserType
from crawlee.fingerprint_suite import FingerprintGenerator, HeaderGenerator
from crawlee.proxy_configuration import ProxyInfo
from crawlee._utils.context import ensure_context
from playwright.async_api import Browser, BrowserContext
from pathlib import Path
from ..models import User


class PlaywrightBrowserController(OldPlaywrightBrowserController):
    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        browser: Browser | PlaywrightPersistentBrowser,
        *,
        max_open_pages_per_browser: int = 20,
        use_incognito_pages: bool = False,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
        fingerprint_generator: FingerprintGenerator | None = None,
        fingerprint_mapping: Mapping[str, Fingerprint] | None = None,
    ) -> None:
        super().__init__(
            browser=browser,
            max_open_pages_per_browser=max_open_pages_per_browser,
            use_incognito_pages=use_incognito_pages,
            header_generator=header_generator,
            fingerprint_generator=fingerprint_generator,
        )
        self._fingerprint_mapping = fingerprint_mapping or {}

    def get_fingerprint(self, session_id: str) -> Fingerprint:
        """Get the fingerprint for the browser controller."""
        return self._fingerprint_mapping[session_id]

    @override
    async def _create_browser_context(
        self,
        browser_new_context_options: Mapping[str, Any] | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> BrowserContext:
        if browser_new_context_options is None:
            browser_new_context_options = {}

        fingerprint = (
            self.get_fingerprint(proxy_info.session_id) if proxy_info else None
        )

        return await AsyncNewContext(
            browser=self._browser,
            fingerprint=fingerprint,
            **browser_new_context_options,
        )


class PlaywrightBrowserPlugin(OldPlaywrightBrowserPlugin):
    def __init__(
        self,
        *,
        browser_type: BrowserType = "chromium",
        user_data_dir: str | Path | None = None,
        browser_launch_options: dict[str, Any] | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        max_open_pages_per_browser: int = 1,
        use_incognito_pages: bool = False,
        fingerprint_generator: FingerprintGenerator | None = None,
        fingerprint_mapping: Mapping[str, Fingerprint] | None = None,
    ) -> None:
        super().__init__(
            browser_type=browser_type,
            user_data_dir=user_data_dir,
            browser_launch_options=browser_launch_options,
            browser_new_context_options=browser_new_context_options,
            max_open_pages_per_browser=max_open_pages_per_browser,
            use_incognito_pages=use_incognito_pages,
            fingerprint_generator=fingerprint_generator,
        )
        self.fingerprint_mapping = fingerprint_mapping or {}

    @ensure_context
    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        controller = await super().new_browser()
        new_controller = PlaywrightBrowserController(
            controller._browser,  # noqa W0212
            use_incognito_pages=self._use_incognito_pages,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
            fingerprint_generator=self._fingerprint_generator,
            fingerprint_mapping=self.fingerprint_mapping,
        )

        return new_controller

    @classmethod
    def with_user_fingerprints(
        cls, users: list[User], headless: bool
    ) -> "PlaywrightBrowserPlugin":
        fingerprint_mapping = {}
        for user in users:
            data = user.fingerprint
            data["screen"] = ScreenFingerprint(**data["screen"])
            data["navigator"] = NavigatorFingerprint(**data["navigator"])
            if "video_card" in data:
                data["video_card"] = VideoCard(**data["video_card"])
            fingerprint_mapping[user.session_id] = Fingerprint(**data)

        return cls(
            browser_type="chromium",
            fingerprint_mapping=fingerprint_mapping,
            browser_launch_options={"headless": headless, "args": ["--no-sandbox"]},
        )


class BrowserPool(OldBrowserPool):
    @classmethod
    def with_default_plugin(
        cls,
        *,
        browser_type: BrowserType | None = None,
        user_data_dir: str | Path | None = None,
        browser_launch_options: Mapping[str, Any] | None = None,
        browser_new_context_options: Mapping[str, Any] | None = None,
        headless: bool | None = None,
        fingerprint_generator: FingerprintGenerator | None = None,
        use_incognito_pages: bool | None = False,
        fingerprint_mapping: Mapping[str, Fingerprint] | None = None,
        **kwargs: Any,
    ) -> "BrowserPool":
        plugin_options: dict = defaultdict(dict)
        plugin_options["browser_launch_options"] = browser_launch_options or {}
        plugin_options["browser_new_context_options"] = (
            browser_new_context_options or {}
        )

        if headless is not None:
            plugin_options["browser_launch_options"]["headless"] = headless

        if use_incognito_pages is not None:
            plugin_options["use_incognito_pages"] = use_incognito_pages

        if browser_type:
            plugin_options["browser_type"] = browser_type

        if user_data_dir:
            plugin_options["user_data_dir"] = user_data_dir

        plugin = PlaywrightBrowserPlugin(
            **plugin_options,
            fingerprint_generator=fingerprint_generator,
            fingerprint_mapping=fingerprint_mapping,
        )
        return cls(plugins=[plugin], **kwargs)
