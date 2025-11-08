import random
import string
from http.cookies import SimpleCookie
from collections.abc import Mapping


def generate_run_id() -> str:
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(15)
    )


def extract_cookies(headers: Mapping[str, str]) -> dict[str, str]:
    """
    Return cookies from either 'Cookie' (request) or 'Set-Cookie' (response)
    headers as a dict[name -> value].
    """
    cookies: dict[str, str] = {}

    # Request cookies (all in one header)
    if "cookie" in headers:
        jar = SimpleCookie()
        jar.load(headers["cookie"])
        cookies.update({k: v.value for k, v in jar.items()})

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
            cookies.update({k: v.value for k, v in jar.items()})

    return cookies
