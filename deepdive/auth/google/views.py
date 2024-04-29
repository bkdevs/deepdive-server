from allauth.socialaccount.providers.google.views import GoogleOAuth2Adapter

from ..utils import get_redirect_uri
from ..views import OAuth2LoginView


class GoogleLoginView(OAuth2LoginView):
    adapter_class = GoogleOAuth2Adapter

    def _get_url_params(self, request):
        return {
            "redirect_uri": get_redirect_uri(self.adapter_class.provider_id),
            "client_id": self._get_client_id(request),
            "scope": "openid email profile",
            "access_type": "offline",
            "prompt": "consent",
            "response_type": "code",
        }
