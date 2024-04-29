"""
ASGI config for server project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/howto/deployment/asgi/
"""

import os
from django.core.asgi import get_asgi_application

# NOTE: the order of these imports are important!
# https://github.com/django/channels/issues/1564#issuecomment-722354397
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "server.settings")
asgi_app = get_asgi_application()

import deepdive.routing
from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.security.websocket import AllowedHostsOriginValidator
from django.core.asgi import get_asgi_application


application = ProtocolTypeRouter(
    {
        "http": asgi_app,
        "websocket": AllowedHostsOriginValidator(
            AuthMiddlewareStack(URLRouter(deepdive.routing.websocket_urlpatterns))
        ),
    }
)
