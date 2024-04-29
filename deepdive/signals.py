from django.dispatch import receiver
from django.db.models.signals import pre_delete

from deepdive.models import DatabaseFile


@receiver(pre_delete, sender=DatabaseFile)
def delete_s3_file(sender, instance, **kwargs):
    instance.file.delete()
