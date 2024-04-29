from django.urls import path

import deepdive.consumers as consumers

websocket_urlpatterns = [
    path("sessions/<uuid:uuid>/", consumers.DeepDiveConsumer.as_asgi())
]
