from django.urls import include, path
from rest_framework import routers

from .views import (
    DatabaseViewSet,
    ExportSharedReportView,
    ExportVisualizationView,
    ListSessionMessages,
    ListSharedSessionMessages,
    ListSharedSessionVisualizations,
    MergeTables,
    PreviewTables,
    SessionViewSet,
    SharedSessionViewSet,
    ListVisualizationsView,
    UpdateDatabaseFileView,
    UpdateVisualization,
)

router = routers.DefaultRouter()
router.register(r"sessions", SessionViewSet, basename="session")
router.register(r"databases", DatabaseViewSet, basename="database")

urlpatterns = [
    path("", include(router.urls)),
    path("auth/", include("deepdive.auth.urls")),
    path("sessions/<uuid:session_id>/messages/", ListSessionMessages.as_view()),
    path("report/<uuid:session_id>/", ListVisualizationsView.as_view()),
    path("visualizations/<uuid:visualization_id>/", UpdateVisualization.as_view()),
    path("preview_tables/", PreviewTables.as_view()),
    path("merge_tables/", MergeTables.as_view()),
    path("database_file/<uuid:database_file_id>/", UpdateDatabaseFileView.as_view()),
    path("share/<uuid:session_id>/", SharedSessionViewSet.as_view()),
    path("share/<uuid:session_id>/messages/", ListSharedSessionMessages.as_view()),
    path("share/<uuid:session_id>/report/", ListSharedSessionVisualizations.as_view()),
    path("export_shared_report/<uuid:session_id>/", ExportSharedReportView.as_view()),
    path("export_visualization/<uuid:viz_id>/", ExportVisualizationView.as_view()),
]
