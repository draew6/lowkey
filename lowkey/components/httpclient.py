from crawlee.http_clients import HttpxHttpClient as OriginalHttpxHttpClient
import httpx


class HttpxHttpClient(OriginalHttpxHttpClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_hooks = {"request": [], "response": []}

    def _get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        client: httpx.AsyncClient = super()._get_client(proxy_url)
        client.event_hooks = self.event_hooks
        return client

    def add_hooks(self, request_hooks=None, response_hooks=None):
        if request_hooks:
            self.event_hooks["request"] = request_hooks
        if response_hooks:
            self.event_hooks["response"] = response_hooks

    @classmethod
    def get_client_with_hooks(cls, hooks: list = None):
        client = cls()
        client.add_hooks(request_hooks=hooks)
        return client
