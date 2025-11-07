from dataclasses import dataclass
from datetime import datetime, UTC
import os
import sys
from importlib.metadata import version, PackageNotFoundError


def get_module_version(module_name: str) -> str:
    try:
        return version(module_name)
    except PackageNotFoundError:
        return "unknown"


def get_python_version() -> str:
    return ".".join(map(str, sys.version_info[:3]))


def get_git_commit() -> str | None:
    return os.getenv("SOURCE_COMMIT")


@dataclass
class ActorInfo:
    name: str
    git_commit: str
    python_version: str
    fastlet_version: str

    @classmethod
    def get_info(cls, name: str):
        return cls(
            name=name,
            git_commit=get_git_commit(),
            python_version=get_python_version(),
            fastlet_version=get_module_version("fastlet"),
        )


@dataclass
class ScraperInfo(ActorInfo):
    crawlee_version: str
    playwright_version: str
    blackcat_version: str

    @classmethod
    def get_info(cls, name: str):
        parent = cls.__mro__[1]  # ActorInfo
        base = parent.get_info(name)
        return cls(
            name=name,
            git_commit=base.git_commit,
            python_version=base.python_version,
            fastlet_version=base.fastlet_version,
            crawlee_version=get_module_version("crawlee"),
            playwright_version=get_module_version("playwright"),
            blackcat_version=get_module_version("blackcat"),
        )


@dataclass
class RunInfo:
    requested_by: str
    requested_at: str
    actor_name: str
    git_commit: str

    @classmethod
    def get_info(cls, requested_by: str, actor_name: str):
        return cls(
            requested_by=requested_by,
            requested_at=datetime.now(UTC).isoformat(),
            actor_name=actor_name,
            git_commit=get_git_commit(),
        )


@dataclass
class ParserInfo(ActorInfo):
    bs4_version: float
    pydantic_version: float
