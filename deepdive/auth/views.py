from abc import ABC, abstractmethod
from urllib.parse import urlencode

from allauth.socialaccount.providers.oauth2.client import OAuth2Client
from dj_rest_auth.registration.views import SocialLoginView
from django.urls.exceptions import NoReverseMatch
from rest_framework import status, views
from rest_framework.response import Response

from .serializers import OAuth2LoginSerializer


class OAuth2LoginView(SocialLoginView, ABC):
    """
    Base view for OAuth2 end-points.
    """

    adapter_class = None
    client_class = OAuth2Client
    serializer_class = OAuth2LoginSerializer
    authentication_classes = []

    def get(self, request, *args, **kwargs):
        data = {"authorization_url": self._construct_authorization_url(request)}
        return Response(data=data, status=status.HTTP_200_OK)

    def post(self, request, *args, **kwargs):
        try:
            return super().post(request, args, kwargs)
        except NoReverseMatch:
            # When the retrieved email already registered via regular signup,
            # allauth returns HTTP redirect to its customized login page using
            # reverse which fails since we do not register allauth views.
            # There isn't a good way to catch this other than re-implementing
            # the entire serializer.
            return Response(
                "A user is already registered with this e-mail address.",
                status.HTTP_409_CONFLICT,
            )

    def _construct_authorization_url(self, request):
        base_url = self.adapter_class.authorize_url
        params = self._get_url_params(request)
        return f"{base_url}?{urlencode(params)}"

    def _get_client_id(self, request):
        adapter = self.adapter_class(request)
        app = adapter.get_provider().get_app(request)
        return app.client_id

    @abstractmethod
    def _get_url_params(self, request):
        pass


class DeleteAccount(views.APIView):
    """
    View to delete DeepDive user account.
    """

    def delete(self, request, *args, **kwargs):
        user=self.request.user
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)