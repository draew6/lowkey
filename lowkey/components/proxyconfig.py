from crawlee.proxy_configuration import ProxyConfiguration as OriginalProxyConfiguration
from ..models.user import User


class ProxyConfiguration(OriginalProxyConfiguration):
    def __init__(self, users: list[User]):
        self.users = {user.session_id: user for user in users}
        new_url_function = (
            lambda session_id, request: self.users[session_id].proxy_ip
            if session_id in self.users
            else None
        )
        super().__init__(new_url_function=new_url_function)
