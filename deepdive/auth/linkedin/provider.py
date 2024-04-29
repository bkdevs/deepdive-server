from allauth.socialaccount.providers.linkedin_oauth2.provider import (
    LinkedInOAuth2Provider,
)


class LinkedInProvider(LinkedInOAuth2Provider):
    """
    Custom provider for LinkedIn to accomodate the new API endpoint.
    See deepdive.auth.linkedin.adapter.LinkedInAdapter.
    """

    def extract_uid(self, data):
        return data["sub"]

    def extract_common_fields(self, data):
        return {
            "fist_name": data["fist_name"],
            "last_name": data["last_name"],
            "email": data["email"],
        }
