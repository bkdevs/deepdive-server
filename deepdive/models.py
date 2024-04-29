from uuid import uuid4

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils.translation import gettext_lazy

from deepdive.auth.models import DeepDiveUser
from deepdive.schema import DatabaseSchema


def get_upload_path(instance, filename):
    return f"{instance.user}/{instance.timestamp}/{filename}"


class DatabaseType(models.TextChoices):
    SQLITE = "sqlite", gettext_lazy("SQLite")
    SNOWFLAKE = "snowflake", gettext_lazy("Snowflake")
    BIGQUERY = "bigquery", gettext_lazy("BigQuery")
    MYSQL = "mysql", gettext_lazy("MySQL")
    CSV = "csv", gettext_lazy("CSV")
    EXCEL = "excel", gettext_lazy("Excel")
    PARQUET = "parquet", gettext_lazy("Parquet")


class Database(models.Model):
    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    database_type = models.CharField(max_length=10, choices=DatabaseType.choices)
    name = models.CharField(max_length=512)
    user = models.ForeignKey(to=DeepDiveUser, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    schema = models.TextField(
        null=False, blank=False
    )  # JSON-encoded field, using DatabaseSchema.model_dump_json()
    starter_questions = ArrayField(models.TextField(), size=10, null=True, blank=True)

    # user/pass auth based fields
    username = models.CharField(max_length=512, blank=True, null=True)
    password = models.CharField(max_length=512, blank=True, null=True)

    # snowflake fields
    snowflake_account = models.CharField(max_length=512, blank=True, null=True)
    snowflake_database = models.CharField(max_length=512, blank=True, null=True)
    snowflake_schema = models.CharField(max_length=512, blank=True, null=True)

    # bigquery fields
    bigquery_dataset_id = models.CharField(max_length=512, blank=True, null=True)

    def get_schema(self):
        return DatabaseSchema.model_validate_json(self.schema)


class DatabaseFile(models.Model):
    """
    A model representing a file (e.g. CSV or Excel) to imported into DB.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    user = models.ForeignKey(to=DeepDiveUser, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)

    # by default, files do not get persisted in DB for performance reasons
    # instead, a path to the file will be persisted
    file = models.FileField(upload_to=get_upload_path)

    # database is optional, it is possible to have orphaned DatabaseFiles
    # if a user uploads a file but terminates before finishing onboarding
    database = models.ForeignKey(
        to=Database,
        related_name="files",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    # field to store extra arguments required to parse the file
    configs = models.JSONField(blank=True, null=True)


class Session(models.Model):
    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    name = models.CharField(max_length=512)
    user = models.ForeignKey(to=DeepDiveUser, on_delete=models.CASCADE)
    database = models.ForeignKey(to=Database, on_delete=models.CASCADE)
    tables = ArrayField(models.CharField(max_length=512), size=10)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Session: {self.id}"


class Visualization(models.Model):
    """
    A model representing visualization in DeepDive report.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    session = models.ForeignKey(
        to=Session, on_delete=models.CASCADE, related_name="visualizations"
    )
    title = models.TextField()
    question = models.TextField()
    sql_query = models.TextField()
    data = models.TextField()
    visualization_spec = models.TextField()
    error_message = models.TextField(null=True, blank=True)

    # for now, order viz in report by timestamp
    # add grid location field later for editing report layout
    timestamp = models.DateTimeField(auto_now_add=True)


class Message(models.Model):
    class MessageType(models.TextChoices):
        QUESTION = "Q", gettext_lazy("Question")
        RESPONSE = "R", gettext_lazy("Response")

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    session = models.ForeignKey(
        to=Session, on_delete=models.CASCADE, related_name="messages"
    )
    message_type = models.CharField(
        max_length=1, choices=MessageType.choices, default=MessageType.QUESTION
    )

    question = models.TextField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    sql_query = models.TextField(null=True, blank=True)
    data = models.TextField(
        null=True, blank=True
    )  # JSON-encoded field, using DataFrame.to_json
    visualization_spec = models.TextField(
        null=True, blank=True
    )  # JSON-encoded field, using VizSpec.model_dump_json()

    timestamp = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.message_type} [{self.timestamp}]"


class SharedDatabase(models.Model):
    """
    A model representing shared database.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    database_type = models.CharField(max_length=10, choices=DatabaseType.choices)
    name = models.CharField(max_length=512)
    timestamp = models.DateTimeField()
    schema = models.TextField()


class SharedSession(models.Model):
    """
    A model representing shared sessions.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    name = models.CharField(max_length=512)
    database = models.ForeignKey(to=SharedDatabase, on_delete=models.CASCADE)
    tables = ArrayField(models.CharField(max_length=512), size=10)


class SharedMessage(models.Model):
    """
    A model representing shared message. When a user creates a shareable link for
    a session, all messages are folked to preserve the current state.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    session = models.ForeignKey(
        to=SharedSession, on_delete=models.CASCADE, related_name="messages"
    )
    message_type = models.CharField(
        max_length=1,
        choices=Message.MessageType.choices,
        default=Message.MessageType.QUESTION,
    )
    question = models.TextField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    sql_query = models.TextField(null=True, blank=True)
    data = models.TextField(null=True, blank=True)
    visualization_spec = models.TextField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)


class SharedVisualization(models.Model):
    """
    A model representing shared visualization in report.
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    session = models.ForeignKey(
        to=SharedSession, on_delete=models.CASCADE, related_name="visualizations"
    )
    title = models.TextField()
    question = models.TextField()
    sql_query = models.TextField()
    data = models.TextField()
    visualization_spec = models.TextField()
    error_message = models.TextField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)


class UnparsedQuery(models.Model):
    """
    The set of unparsed SQL queries and expressions

    We _should_ be able to parse a SQL query fully into a SqlTree and back
    This set is what our parsing fails for as of now, i.e, what we need to work on
    """

    id = models.UUIDField(default=uuid4, primary_key=True, editable=False)
    query = models.TextField(null=True, blank=True)
