import requests
from allauth.socialaccount.providers.linkedin_oauth2.views import LinkedInOAuth2Adapter

from .provider import LinkedInProvider


class LinkedInAdapter(LinkedInOAuth2Adapter):
    """
    Custom adapter for LinkedIn inheriting from allauth LinkedInOAuth2Adapter which
    still uses the old deprecated API endpoint:
    (https://learn.microsoft.com/en-us/linkedin/consumer/integrations/self-serve/sign-in-with-linkedin)

    The new API endpoint are defined here:
    (https://learn.microsoft.com/en-us/linkedin/consumer/integrations/self-serve/sign-in-with-linkedin-v2)
    """

    user_info_url = "https://api.linkedin.com/v2/userinfo"

    def get_provider(self):
        return LinkedInProvider(self.request)

    def get_user_info(self, token):
        response = requests.get(
            self.user_info_url, headers=self._get_headers(token), timeout=2.50
        )
        response.raise_for_status()
        return self._process_response(response.json())

    def _get_headers(self, token):
        headers = {}
        headers.update(self.get_provider().get_settings().get("HEADERS", {}))
        headers["Authorization"] = " ".join(["Bearer", token.token])
        return headers

    def _process_response(self, raw_json):
        return {
            "sub": raw_json["sub"],
            "fist_name": raw_json["given_name"],
            "last_name": raw_json["family_name"],
            "email": raw_json["email"],
        }
