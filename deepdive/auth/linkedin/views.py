from ..utils import get_redirect_uri
from ..views import OAuth2LoginView
from .adapters import LinkedInAdapter


class LinkedInLoginView(OAuth2LoginView):
    adapter_class = LinkedInAdapter

    def _get_url_params(self, request):
        return {
            "redirect_uri": get_redirect_uri(self.adapter_class.provider_id),
            "client_id": self._get_client_id(request),
            "scope": "openid email profile",
            "response_type": "code",
        }
