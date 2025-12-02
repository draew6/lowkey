from typing import Literal, Self

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator


class ScraperSettings(BaseSettings):
    auth_refresh_token: str
    project_name: str
    minio_endpoint: str | None = None
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    minio_bucket_name: str | None = None

    storage_type: Literal["local", "minio"] = "minio"

    @model_validator(mode="after")
    def validate_storage(self) -> Self:
        if self.storage_type == "local":
            return self

        missing = [
            f
            for f in (
                "minio_endpoint",
                "minio_access_key",
                "minio_secret_key",
                "minio_bucket_name",
            )
            if not getattr(self, f)
        ]

        if missing:
            raise ValueError(f"storage_type=minio requires: {', '.join(missing)}")
        return self

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class ParserSettings(ScraperSettings):
    duck_pg_host: str
    duck_pg_user: str
    duck_pg_password: str
    duck_pg_dbname: str
    duck_pg_port: int

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
