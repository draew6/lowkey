import asyncio
import random
import string
from http.cookies import SimpleCookie
from collections.abc import Mapping
from crawlee.sessions import SessionCookies


def generate_run_id() -> str:
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(15)
    )


def extract_cookies(headers: Mapping[str, str], domain: str) -> list[dict[str, str]]:
    """
    Return cookies from either 'Cookie' (request) or 'Set-Cookie' (response)
    headers as a dict[name -> value].
    """
    cookies: list[dict[str, str]] = []

    # Request cookies (all in one header)
    if "cookie" in headers:
        raw = headers["cookie"]
        jar = SimpleCookie()
        jar.load(raw)
        cookies += [
            s | {"value": s["value"].value, "domain": s.get("domain") or domain}
            for s in SessionCookies(jar).get_cookies_as_dicts()
        ]

    # Response cookies (usually multiple Set-Cookie headers, but we only have one string here)
    if "set-cookie" in headers:
        raw = headers["set-cookie"]
        # Try newline split first, else just treat as one
        parts = [
            p.strip() for p in raw.replace("\r\n", "\n").split("\n") if p.strip()
        ] or [raw]
        for part in parts:
            jar = SimpleCookie()
            jar.load(part)
            cookies += [
                s | {"value": s["value"].value, "domain": s.get("domain") or domain}
                for s in SessionCookies(jar).get_cookies_as_dicts()
            ]
    return cookies

async def random_sleep(seconds: float):
    sleep_time = random.uniform(seconds / 2, seconds * 1.5)
    await asyncio.sleep(sleep_time)