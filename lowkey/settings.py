from pydantic_settings import BaseSettings, SettingsConfigDict


class ScraperSettings(BaseSettings):
    auth_refresh_token: str
    project_name: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
