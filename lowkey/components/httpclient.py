from crawlee.http_clients import HttpxHttpClient as OriginalHttpxHttpClient
import httpx


async def extract_phase(request: httpx.Request):
    phase = request.headers.pop("lk-phase", None)
    work_type = request.headers.pop("lk-work-type", None)
    if phase:
        request.extensions["lk-phase"] = phase
    if work_type:
        request.extensions["lk-work-type"] = work_type


class HttpxHttpClient(OriginalHttpxHttpClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_hooks = {"request": [], "response": []}

    def _get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        client: httpx.AsyncClient = super()._get_client(proxy_url)
        client.event_hooks = self.event_hooks
        client._timeout = httpx.Timeout(timeout=10.0)
        return client

    def add_hooks(self, request_hooks=None, response_hooks=None):
        if request_hooks:
            self.event_hooks["request"] = request_hooks
        if response_hooks:
            self.event_hooks["response"] = response_hooks
        self.event_hooks["request"].insert(0, extract_phase)

    @classmethod
    def get_client_with_hooks(cls, hooks: list = None, follow_redirects: bool = True):
        client = cls(follow_redirects=follow_redirects)
        client.add_hooks(request_hooks=hooks)
        return client
