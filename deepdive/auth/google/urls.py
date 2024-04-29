from django.urls import path

from .views import GoogleLoginView

urlpatterns = [
    path("login/", GoogleLoginView.as_view()),
]
