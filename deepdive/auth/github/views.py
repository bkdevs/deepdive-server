from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter

from ..utils import get_redirect_uri
from ..views import OAuth2LoginView


class GithubLoginView(OAuth2LoginView):
    adapter_class = GitHubOAuth2Adapter

    def _get_url_params(self, request):
        return {
            "redirect_uri": get_redirect_uri(self.adapter_class.provider_id),
            "client_id": self._get_client_id(request),
            "scope": "user",
        }
