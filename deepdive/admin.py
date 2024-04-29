from django.contrib import admin
from django.contrib.auth.models import Group

from deepdive.auth.models import DeepDiveUser
from deepdive.models import (
    Database,
    DatabaseFile,
    Message,
    Session,
    SharedDatabase,
    SharedMessage,
    SharedSession,
    SharedVisualization,
    UnparsedQuery,
    Visualization,
)

admin.site.register(DeepDiveUser)
admin.site.register(Session)
admin.site.register(Database)
admin.site.register(DatabaseFile)
admin.site.register(Message)
admin.site.register(SharedDatabase)
admin.site.register(SharedSession)
admin.site.register(SharedMessage)
admin.site.register(SharedVisualization)
admin.site.register(UnparsedQuery)
admin.site.register(Visualization)

# unregister the Group model from admin since we no longer use Django's default auth models
admin.site.unregister(Group)
