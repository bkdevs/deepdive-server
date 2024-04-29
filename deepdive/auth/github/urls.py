from django.urls import path

from .views import GithubLoginView

urlpatterns = [
    path("login/", GithubLoginView.as_view()),
]
