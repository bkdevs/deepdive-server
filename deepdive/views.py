import io
import json
from pathlib import Path
from typing import List

from asgiref.sync import async_to_sync
import pandas as pd
from django.core.files.base import ContentFile
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from rest_framework import generics, status, views, viewsets
from rest_framework.exceptions import ErrorDetail, ValidationError
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.settings import api_settings

from deepdive.database import fetch_schema, preview_table, preview_tables, validate_db
from deepdive.database.file_based_client_helper import (
    get_db_type,
    merge_db_files,
    sanitize_database_schema,
    sanitize_table_configs,
    sanitize_table_name,
)
from deepdive.gpt import get_gpt_client
from deepdive.gpt.openai_client import OpenAIClient
from deepdive.models import (
    Database,
    DatabaseFile,
    Message,
    Session,
    SharedDatabase,
    SharedMessage,
    SharedSession,
    SharedVisualization,
    Visualization,
)
from deepdive.serializers import (
    DatabaseListSerializer,
    DatabaseReadSerializer,
    DatabaseWriteSerializer,
    MessageSerializer,
    SessionDetailSerializer,
    SessionListSerializer,
    SharedMessageSerializer,
    SharedSessionSerializer,
    SharedVisualizationsSerializer,
    VisualizationSerializer,
)


class DatabaseViewSet(viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        if self.action == "create":
            return DatabaseWriteSerializer
        elif self.action == "list":
            return DatabaseListSerializer
        return DatabaseReadSerializer

    def get_queryset(self):
        user = self.request.user
        return Database.objects.filter(user=user).order_by("-timestamp")

    def create(self, request, *args, **kwargs):
        serializer = DatabaseWriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        database_args = {**serializer.validated_data}
        database_args["user"] = self.request.user
        database_file_ids = database_args.pop("database_files", [])
        database = Database(**database_args)
        try:
            validate_db(database)
        except Exception as e:
            raise ValidationError(
                {api_settings.NON_FIELD_ERRORS_KEY: ErrorDetail(str(e))}
            )

        schema = (
            sanitize_database_schema(request.data["schema"])
            if "schema" in request.data
            else fetch_schema(database)
        )
        schema.foreign_keys = get_gpt_client(
            "zero-shot", "gpt-3.5-turbo", schema
        ).generate_foreign_keys(schema)

        try:
            client = OpenAIClient(schema)
            questions = async_to_sync(client.generate_questions_async)()
            database.starter_questions = questions
        except Exception as e:
            print(f"Failed to generate starter questions: {str(e)}")

        database.schema = schema.model_dump_json(exclude_none=True)
        database.save()

        table_configs = json.loads(request.data["table_configs"])
        for database_file_id in database_file_ids:
            db_file = DatabaseFile.objects.get(id=database_file_id)
            db_file.database = database
            configs = json.loads(db_file.configs) if db_file.configs else {}
            for table_name, config in table_configs.items():
                if config["file_id"] == str(database_file_id):
                    updated_table_name = sanitize_table_name(config["new_name"])
                    if table_name not in configs:
                        configs[table_name] = {}
                        configs[table_name]["excel_params"] = {}
                    configs[table_name]["name"] = updated_table_name
            db_file.configs = json.dumps(configs)
            db_file.save()

        headers = self.get_success_headers(serializer.data)
        return Response(
            DatabaseReadSerializer(database).data,
            status=status.HTTP_201_CREATED,
            headers=headers,
        )


class PreviewTables(views.APIView):
    """
    Preview tables uploads DB files and returns a preview of them in a single call
    """

    def post(self, request, *args, **kwargs):
        file = request.FILES["database_file"]
        previews = preview_tables(get_db_type(Path(file.name).suffix), file)

        # reset file after previewing
        file.seek(0)

        db_file = DatabaseFile(file=file, user=self.request.user)
        db_file.save()

        return Response(
            {
                "fileName": file.name,
                "fileId": db_file.id,
                "tablePreviews": [
                    preview.model_dump(exclude_none=True) for preview in previews
                ],
            },
            status=status.HTTP_200_OK,
        )


class MergeTables(views.APIView):
    """
    Merge given DatabaseFiles with the same schema into one table and
    return a preview of the merged table.
    """

    def post(self, request, *args, **kwargs):
        db_files = []
        for file_id in request.data:
            db_files.append(DatabaseFile.objects.get(id=file_id))

        merged_db_file = DatabaseFile(user=self.request.user)
        merged_db_file.save()
        merged_file = merge_db_files(db_files)
        merged_db_file.file.save(
            "DataTable.csv", ContentFile(merged_file.getvalue().encode())
        )

        previews = preview_tables("csv", merged_db_file.file)
        return Response(
            {
                "fileName": "DataTable.csv",
                "fileId": merged_db_file.id,
                "tablePreviews": [
                    preview.model_dump(exclude_none=True) for preview in previews
                ],
            },
            status=status.HTTP_200_OK,
        )


class UpdateDatabaseFileView(views.APIView):
    permission_classes = [IsAuthenticated]

    def patch(self, request, *args, **kwargs):
        database_file_id = self.kwargs["database_file_id"]
        db_file = DatabaseFile.objects.get(id=database_file_id)
        sanitized_config = sanitize_table_configs(request.data)

        sanitized_orig_table_name = next(iter(sanitized_config))
        preview = preview_table(
            db_file.file,
            sanitized_orig_table_name,
            sanitized_config[sanitized_orig_table_name],
        )
        self._save_config(db_file, sanitized_config)
        return Response(
            {
                "preview": preview.model_dump(exclude_none=True),
            },
            status=status.HTTP_200_OK,
        )

    def _save_config(self, db_file, sanitized_config):
        if db_file.configs:
            configs = json.loads(db_file.configs)
        else:
            configs = {}
        for key, val in sanitized_config.items():
            configs[key] = val.model_dump()
        db_file.configs = json.dumps(configs)
        db_file.save()


class SessionViewSet(viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        if self.action == "list":
            return SessionListSerializer
        return SessionDetailSerializer

    def get_queryset(self):
        user = self.request.user
        return Session.objects.filter(user=user)

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset().all().order_by("-timestamp")
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def perform_create(self, serializer):
        database = Database.objects.get(id=self.request.data["database_id"])
        tables = [table.name for table in database.get_schema().tables]
        serializer.save(
            user=self.request.user, name=f"{database.name} session", tables=tables
        )


class ListSessionMessages(generics.ListAPIView):
    serializer_class = MessageSerializer

    def get_queryset(self):
        session_id = self.kwargs["session_id"]
        if not session_id:
            return Message.objects.none()
        session = get_object_or_404(Session, id=session_id)
        if session.user_id != self.request.user.id:
            return Message.objects.none()
        return session.messages.all().order_by("timestamp")


class SharedSessionViewSet(generics.CreateAPIView, generics.RetrieveAPIView):
    serializer_class = SharedSessionSerializer

    def create(self, request, *args, **kwargs):
        session_id = self.kwargs["session_id"]
        session = Session.objects.select_related("database").get(id=session_id)

        # create a shared db
        shared_database = SharedDatabase(
            database_type=session.database.database_type,
            name=session.database.name,
            timestamp=session.database.timestamp,
            schema=session.database.schema,
        )
        shared_database.save()

        # create a shared session
        shared_session = SharedSession(
            name=session.name,
            database=shared_database,
            tables=session.tables,
        )
        shared_session.save()

        # folk all messages
        for message in session.messages.all().order_by("timestamp"):
            folked_message = SharedMessage(
                session=shared_session,
                message_type=message.message_type,
                question=message.question,
                error_message=message.error_message,
                sql_query=message.sql_query,
                data=message.data,
                visualization_spec=message.visualization_spec,
            )
            folked_message.save()

        # folk all visualizations in report
        for viz in session.visualizations.all().order_by("timestamp"):
            folked_viz = SharedVisualization(
                session=shared_session,
                title=viz.title,
                question=viz.question,
                sql_query=viz.sql_query,
                data=viz.data,
                visualization_spec=viz.visualization_spec,
                error_message=viz.error_message,
            )
            folked_viz.save()

        serializer = self.get_serializer(shared_session)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def get(self, request, *args, **kwargs):
        session_id = self.kwargs["session_id"]
        session = get_object_or_404(SharedSession, id=session_id)
        serializer = self.get_serializer(session)
        return Response(serializer.data, status=status.HTTP_200_OK)


class ListSharedSessionMessages(generics.ListAPIView):
    permission_classes = [AllowAny]
    serializer_class = SharedMessageSerializer

    def get_queryset(self):
        session_id = self.kwargs["session_id"]
        if not session_id:
            return Message.objects.none()
        session = get_object_or_404(SharedSession, id=session_id)
        return session.messages.all().order_by("timestamp")


class ListSharedSessionVisualizations(generics.ListAPIView):
    permission_classes = [AllowAny]
    serializer_class = SharedVisualizationsSerializer

    def get_queryset(self):
        session_id = self.kwargs["session_id"]
        if not session_id:
            return Message.objects.none()
        session = get_object_or_404(SharedSession, id=session_id)
        return session.visualizations.all().order_by("timestamp")


class UpdateVisualization(views.APIView):
    permission_classes = [IsAuthenticated]

    def patch(self, request, *args, **kwargs):
        visualization_id = self.kwargs["visualization_id"]
        if not visualization_id:
            return Visualization.objects.none()
        visualization = get_object_or_404(Visualization, id=visualization_id)
        visualization.title = request.data["title"]
        visualization.save()

        return Response(
            status=status.HTTP_200_OK,
        )


class ListVisualizationsView(generics.ListAPIView):
    serializer_class = VisualizationSerializer

    def get_queryset(self):
        session = Session.objects.get(id=self.kwargs["session_id"])
        return session.visualizations.all().order_by("timestamp")


class ExportSharedReportView(generics.RetrieveAPIView):
    permission_classes = [AllowAny]

    def retrieve(self, request, *args, **kwargs):
        session = SharedSession.objects.prefetch_related("visualizations").get(
            id=self.kwargs["session_id"]
        )
        return export_as_excel(session.visualizations.all())


class ExportVisualizationView(generics.RetrieveAPIView):
    permission_classes = [IsAuthenticated]

    def retrieve(self, request, *args, **kwargs):
        viz = Visualization.objects.get(id=self.kwargs["viz_id"])
        return export_as_excel([viz])


def export_as_excel(
    visualizations: List[Visualization], filename: str = "report"
) -> Response:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        index = 1
        for viz in visualizations:
            data = json.loads(viz.data)["data"]
            df = pd.DataFrame.from_dict(data, orient="columns")
            df.to_excel(writer, sheet_name=f"sheet {index}", index=False)
            index = index + 1

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="{filename}.xlsx"'},
    )
    buffer.seek(0)
    response.write(buffer.read())
    return response
