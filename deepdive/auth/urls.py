from django.urls import include, path

from deepdive.auth.views import DeleteAccount

urlpatterns = [
    path("", include("dj_rest_auth.urls")),
    path("register/", include("dj_rest_auth.registration.urls")),
    path("delete/", DeleteAccount.as_view()),
    path("github/", include("deepdive.auth.github.urls")),
    path("google/", include("deepdive.auth.google.urls")),
    path("linkedin_oauth2/", include("deepdive.auth.linkedin.urls")),
]
