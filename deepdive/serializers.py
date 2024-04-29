import os

from rest_framework import serializers

from deepdive.models import (
    Database,
    Message,
    Session,
    SharedDatabase,
    SharedMessage,
    SharedSession,
    SharedVisualization,
    Visualization,
)


class DatabaseWriteSerializer(serializers.ModelSerializer):
    schema = serializers.CharField(required=False)
    database_files = serializers.ListField(
        child=serializers.UUIDField(), allow_empty=True, default=[]
    )

    class Meta:
        model = Database
        fields = [
            "id",
            "database_type",
            "database_files",
            "name",
            "schema",
            "username",
            "password",
            "bigquery_dataset_id",
            "snowflake_account",
            "snowflake_database",
            "snowflake_schema",
        ]


class DatabaseListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Database
        fields = ["id", "name", "timestamp"]


class FilenameField(serializers.RelatedField):
    def to_representation(self, value):
        return os.path.basename(value.file.name)


class DatabaseReadSerializer(serializers.ModelSerializer):
    """
    Only non-sensitive fields should be set here (i.e, what's okay for the user to see)
    """

    user_username = serializers.CharField(source="user.username", read_only=True)
    files = FilenameField(many=True, read_only=True)
    starter_questions = serializers.ListField(
        child=serializers.CharField(), allow_empty=True, default=[]
    )

    class Meta:
        model = Database
        fields = [
            "id",
            "database_type",
            "name",
            "user_username",
            "timestamp",
            "schema",
            "starter_questions",
            "snowflake_account",
            "snowflake_database",
            "snowflake_schema",
            "bigquery_dataset_id",
            "files",
        ]
        read_only_fields = fields


class SessionListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Session
        fields = ["id", "name", "timestamp"]


class SessionDetailSerializer(serializers.ModelSerializer):
    name = serializers.CharField(required=False)
    tables = serializers.ListField(
        child=serializers.CharField(max_length=512), allow_empty=True, default=[]
    )
    database = DatabaseReadSerializer(read_only=True)
    database_id = serializers.UUIDField(format="hex_verbose", write_only=True)

    class Meta:
        model = Session
        fields = ["id", "name", "database", "database_id", "tables"]


class MessageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Message
        fields = [
            "id",
            "session",
            "message_type",
            "question",
            "sql_query",
            "data",
            "visualization_spec",
            "error_message",
            "timestamp",
        ]
        read_only_fields = ["id", "timestamp"]


class SharedDatabaseSerializer(serializers.ModelSerializer):
    class Meta:
        model = SharedDatabase
        fields = [
            "id",
            "database_type",
            "name",
            "timestamp",
            "schema",
        ]


class SharedSessionSerializer(serializers.ModelSerializer):
    database = SharedDatabaseSerializer()

    class Meta:
        model = SharedSession
        fields = ["id", "name", "database", "tables"]


class SharedMessageSerializer(serializers.ModelSerializer):
    class Meta:
        model = SharedMessage
        fields = [
            "id",
            "session",
            "message_type",
            "question",
            "sql_query",
            "data",
            "visualization_spec",
            "error_message",
            "timestamp",
        ]
        read_only_fields = ["id", "timestamp"]


class VisualizationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Visualization
        fields = [
            "id",
            "title",
            "data",
            "sql_query",
            "visualization_spec",
            "error_message",
        ]


class SharedVisualizationsSerializer(serializers.ModelSerializer):
    class Meta:
        model = SharedVisualization
        fields = [
            "id",
            "title",
            "data",
            "sql_query",
            "visualization_spec",
            "error_message",
        ]
