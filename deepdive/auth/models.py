from uuid import uuid4

from django.contrib.auth.models import (
    AbstractBaseUser,
    BaseUserManager,
    PermissionsMixin,
)
from django.db import models


class DeepDiveUserManager(BaseUserManager):
    """
    UserManager of DeepDiveUser.
    """

    def create_user(self, email, password=None, **kwargs):
        """
        Creates and saves a user with the given email and password.
        """
        if not email:
            raise ValueError("Email not provided")
        email = self.normalize_email(email)
        user = self.model(email=email, **kwargs)
        user.set_password(password)
        user.save()
        return user

    def create_superuser(self, email, password=None, **kwargs):
        """
        Creates and saves a superuser with the given email and password.
        """
        kwargs.setdefault("is_active", True)
        kwargs.setdefault("is_staff", True)
        kwargs.setdefault("is_superuser", True)
        if kwargs.get("is_active") is not True:
            raise ValueError("Superuser should be active")
        if kwargs.get("is_staff") is not True:
            raise ValueError("Superuser should be staff")
        if kwargs.get("is_superuser") is not True:
            raise ValueError("Superuser should have is_superuser=True")
        return self.create_user(email, password, **kwargs)


class DeepDiveUser(AbstractBaseUser, PermissionsMixin):
    """
    A custom user model for DeepDive application.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    email = models.EmailField(max_length=255, unique=True)
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(auto_now_add=True)

    objects = DeepDiveUserManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []
