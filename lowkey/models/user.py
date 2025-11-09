from datetime import datetime
from ..settings import ScraperSettings
from .client import APIClient
from pydantic import BaseModel


class Activity(BaseModel):
    user_id: int
    scraper_name: str
    url: str
    status_code: int | None = None
    error_message: str | None = None
    reported_by: str
    reported_at: datetime


class User(BaseModel):
    proxy_ip: str
    fingerprint: dict
    user_agent: str
    user_id: int
    user_data: dict
    cookies: list[dict]

    @property
    def session_id(self) -> str:
        return f"session-{self.user_id}"

    @staticmethod
    def id_from_session_id(session_id: str) -> int:
        return int(session_id.replace("session-", ""))

    @classmethod
    async def get_users(
        cls, api_client: APIClient, target_id: int, limit: int, pool_size: int
    ) -> list["User"]:
        settings = ScraperSettings()
        refresh_token = settings.auth_refresh_token
        auth_response = await api_client.auth.refresh_access_token(refresh_token)
        api_client.auth.set_access_token(auth_response.access_token)
        api_client.user.set_access_token(auth_response.access_token)
        response = await api_client.user.get_users(target_id, limit, pool_size)
        return [cls(**user.model_dump()) for user in response.users]

    @staticmethod
    async def update_users(api_client: APIClient, users: list["User"]) -> None:
        settings = ScraperSettings()
        refresh_token = settings.auth_refresh_token
        auth_response = await api_client.auth.refresh_access_token(refresh_token)
        api_client.auth.set_access_token(auth_response.access_token)
        api_client.user.set_access_token(auth_response.access_token)
        await api_client.user.save_user_data([user.model_dump() for user in users])

    @staticmethod
    async def report_activities(
        api_client: APIClient, run_id: str, activities: list[Activity]
    ):
        settings = ScraperSettings()
        refresh_token = settings.auth_refresh_token
        auth_response = await api_client.auth.refresh_access_token(refresh_token)
        api_client.auth.set_access_token(auth_response.access_token)
        api_client.user.set_access_token(auth_response.access_token)
        await api_client.user.create_activities(
            [activity.model_dump() for activity in activities], run_id
        )
