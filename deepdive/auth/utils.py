from django.conf import settings


def get_redirect_uri(provider_id):
    protocol = getattr(settings, "PROTOCOL")
    domain = getattr(settings, "DOMAIN")
    return f"{protocol}://{domain}/{provider_id}/callback"
