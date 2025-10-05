from typing import Protocol, Self


class AccessTokenResponse(Protocol):
    access_token: str


class UserResponse(Protocol):
    users: list


class Client(Protocol):
    def set_access_token(self, access_token: str | None): ...


class AuthClient(Client, Protocol):
    async def refresh_access_token(self, refresh_token: str) -> AccessTokenResponse: ...


class UserClient(Client, Protocol):
    async def get_users(
        self, target_id: int, limit: int, pool_size: int
    ) -> UserResponse: ...

    async def save_user_data(self, users: list) -> None: ...


class APIClient(Protocol):
    @property
    def auth(self) -> AuthClient: ...

    @property
    def user(self) -> UserClient: ...

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...
