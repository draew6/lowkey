from typing import Annotated
from lowkey.settings import ScraperSettings
from fastapi import Depends


def get_scraper_settings():
    settings = ScraperSettings()
    return settings


ScraperSettings = Annotated[ScraperSettings, Depends(get_scraper_settings)]
