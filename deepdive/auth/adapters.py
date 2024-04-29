from allauth.account.adapter import DefaultAccountAdapter

from deepdive.auth.emails import ActivationEmail


class DeepDiveAccountAdapter(DefaultAccountAdapter):
    """
    A custom AccountAdapter for DeepDive.
    """

    def clean_email(self, email):
        return email.lower()

    def send_confirmation_mail(self, request, emailconfirmation, signup):
        email = ActivationEmail(request=request, key=emailconfirmation.key)
        email.send(to=[emailconfirmation.email_address.email])
