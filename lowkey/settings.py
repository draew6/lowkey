from pydantic_settings import BaseSettings, SettingsConfigDict


class ScraperSettings(BaseSettings):
    auth_refresh_token: str
    project_name: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket_name: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
