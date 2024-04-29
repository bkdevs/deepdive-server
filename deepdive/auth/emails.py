from django.conf import settings
from templated_mail.mail import BaseEmailMessage


class ActivationEmail(BaseEmailMessage):
    """
    Class responsible for sending activation emails.
    """

    template_name = "email/activation.html"

    def __init__(
        self, request=None, context=None, template_name=None, key=None, *args, **kwargs
    ):
        super(ActivationEmail, self).__init__(
            request, context, template_name, *args, **kwargs
        )
        self.key = key

    def get_context_data(self):
        context = super().get_context_data()
        context["url"] = getattr(settings, "ACTIVATION_URL", "").format(key=self.key)
        return context
