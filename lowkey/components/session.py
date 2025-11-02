from datetime import datetime, UTC
from typing import Literal
from crawlee.events import EventManager
from crawlee.sessions import (
    Session as OriginalSession,
    SessionPool as OriginalSessionPool,
)
from crawlee.sessions._session_pool import CreateSessionFunctionType
import random
import asyncio
from ..models.user import User


class Session(OriginalSession):
    def is_in_discovery_phase(self) -> bool:
        """
        Check if the session is in the discovery phase.
        A session is considered to be in the discovery phase if it didn't arrive at a content interesting to scraper.
        """
        return self.phase == "DISCOVERY"

    def turn_to_final_phase(self) -> None:
        """Turn the session to final phase."""
        self.user_data["phase"] = "FINAL"

    def turn_to_discovery_phase(self) -> None:
        """Turn the session to discovery phase."""
        self.user_data["phase"] = "DISCOVERY"

    @property
    def phase(self) -> Literal["DISCOVERY", "FINAL"]:
        """Get the current phase of the session."""
        return self.user_data.get("phase", "FINAL")


class SessionPool(OriginalSessionPool):
    """
    Custom SessionPool that returns a random session that has not been used in the last `regen_time` seconds to avoid too fast requests from one users.
    After retiring a session, it is removed from the pool and a new session IS NOT created.
    :regen_time: time in seconds after which a session is considered rested and can be reused"""

    def __init__(
        self,
        *,
        users: list[User],
        regen_time: int = 1,
        create_session_settings: dict | None = None,
        create_session_function: CreateSessionFunctionType | None = None,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str | None = None,
        persist_state_key: str = "CRAWLEE_SESSION_POOL_STATE",
    ):
        super().__init__(
            max_pool_size=len(users),
            create_session_settings=create_session_settings,
            create_session_function=create_session_function,
            event_manager=event_manager,
            persistence_enabled=persistence_enabled,
            persist_state_kvs_name=persist_state_kvs_name,
            persist_state_key=persist_state_key,
        )
        self.regen_time = regen_time
        self.last_used: dict[str, datetime] = {}
        self.create_sessions_from_users(users)

    def _get_random_rested_session(self) -> Session | None:
        """Get a random session from the pool."""
        state = self._state.current_value
        if not state.sessions:
            raise ValueError("No sessions available in the pool.")
        rested_sessions = [
            session
            for session in list(state.sessions.values())
            if session.id not in self.last_used
            or (datetime.now(UTC) - self.last_used[session.id]).total_seconds()
            > self.regen_time
        ]
        if not rested_sessions:
            return None
        return random.choice(rested_sessions)

    async def get_random_rested_session(self) -> Session:
        """Try to get a random rested session, waiting if necessary."""
        counter = 0
        start_time = datetime.now(UTC)
        while True:
            session = self._get_random_rested_session()
            if session:
                return session
            counter += 1
            await asyncio.sleep(1)
            if counter == 1:
                print("Waiting for sessions to become available (rest)")
            if (datetime.now(UTC) - start_time).total_seconds() > 30 * 60:
                raise TimeoutError("Timed out waiting for sessions to be created.")

    async def get_session(self) -> Session:
        """Retrieve a random session from the pool.

        This method first ensures the session pool is at its maximum capacity. If the random session is not usable,
        retired sessions are removed and a new session is created and returned.

        Returns:
            The session object.
        """
        session = await self.get_random_rested_session()
        if session.is_usable:
            self.last_used[session.id] = datetime.now(UTC)
            return session

        # If the random session is not usable, clean up and create a new session
        self._remove_retired_sessions()
        session = await self.get_random_rested_session()
        self.last_used[session.id] = datetime.now(UTC)
        return session

    async def get_session_by_id(self, session_id: str) -> Session | None:
        session = self._state.current_value.sessions.get(session_id)

        if not session:
            return None

        if not session.is_usable:
            return None

        self.last_used[session.id] = datetime.now(UTC)
        return session

    def create_sessions_from_users(self, users: list[User]) -> None:
        """Create sessions from a list of users."""
        self._state._default_state.sessions = {  # noqa
            user.session_id: Session(
                id=user.session_id,
                user_data={
                    "proxy_url": user.proxy_ip,  # doesn't have to be here
                    "user_agent": user.user_agent,
                    "cookies": user.cookies,
                }
                | user.user_data,
                cookies=user.cookies,
            )
            for user in users
        }

    def create_users_from_sessions(self) -> list[User]:
        """Create users from the sessions in the pool."""
        state = self._state.current_value
        users = []
        for session in state.sessions.values():
            user = User(
                user_id=User.id_from_session_id(session.id),
                proxy_ip=session.user_data.get("proxy_url", ""),
                user_agent=session.user_data.get("user_agent", ""),
                fingerprint=session.user_data.get("fingerprint", {}),
                cookies=session.user_data.get("cookies", {})
                | {cookie["name"]: cookie["value"] for cookie in session.cookies},
                user_data={
                    k: v
                    for k, v in session.user_data.items()
                    if k not in ["proxy_url", "user_agent", "fingerprint", "cookies", "phase"]
                },
            )
            users.append(user)
        return users
