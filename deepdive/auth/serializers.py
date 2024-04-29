from dj_rest_auth.registration.serializers import SocialLoginSerializer
from dj_rest_auth.serializers import LoginSerializer

from .utils import get_redirect_uri


class DeepDiveLoginSerializer(LoginSerializer):
    """
    Custom login serializer for DeepDive.
    """

    def _validate_email(self, email, password):
        return super()._validate_email(email.lower(), password)


class OAuth2LoginSerializer(SocialLoginSerializer):
    """
    Custom serializer for OAuth 2.0 login flow.
    """

    def set_callback_url(self, view, adapter_class):
        self.callback_url = get_redirect_uri(adapter_class.provider_id)
