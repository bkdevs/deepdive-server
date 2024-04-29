from django.urls import path

from .views import LinkedInLoginView

urlpatterns = [
    path("login/", LinkedInLoginView.as_view()),
]
