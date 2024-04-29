import enum
import json
from typing import Dict, Tuple

from channels.generic.websocket import AsyncWebsocketConsumer
from django.core.serializers.json import DjangoJSONEncoder

from deepdive.models import Message, Session, Visualization
from deepdive.deepdive_client import DeepDiveClient
from deepdive.serializers import MessageSerializer, VisualizationSerializer
from deepdive.viz.parser import parse_spec


class DeepDiveConsumer(AsyncWebsocketConsumer):
    """
    Asynchronous websocket consumer to handle session level requests
    such as processing message, updating visualization, updating report, etc.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        self.session = None
        self.processor = None

    async def connect(self):
        session_id = self.scope["url_route"]["kwargs"]["uuid"]
        self.session = await Session.objects.select_related("database").aget(
            id=session_id
        )
        self.processor = RequestProcessor(self.session)
        await self.processor.initialize_async()
        await self.accept()

    async def receive(self, text_data=None, bytes_data=None):
        response = await self.processor.process_async(text_data)
        await self.send(text_data=response)

    async def disconnect(self, code):
        await self.processor.finalize_async()


class RequestProcessor:
    """
    Internal class to handle websocket requests.
    """

    def __init__(self, session: Session):
        self.session = session
        self.client = DeepDiveClient(self.session)

    async def initialize_async(self):
        await self.client.initialize_async()

    async def process_async(self, text_data: str):
        try:
            request = json.loads(text_data)
            self._validate_request(request)
        except ValueError as ex:
            return self._serialize_response(400, "", "", str(ex))

        action = request["action"]
        try:
            if action == ActionType.PROCESS_QUESTION:
                response, error_message = await self._process_question_async(request)
            elif action == ActionType.PROCESS_SQL_QUERY:
                response, error_message = await self._process_sql_query_async(request)
            elif action == ActionType.UPDATE_VIZ_SPEC:
                response, error_message = await self._update_viz_spec_async(request)
            elif action == ActionType.GENERATE_REPORT:
                response, error_message = await self._generate_report_async()
            elif action == ActionType.ADD_VISUALIZATION:
                response, error_message = await self._add_new_viz_async(request)
            elif action == ActionType.REMOVE_VISUALIZATION:
                response, error_message = await self._remove_viz_async(request)
            elif action == ActionType.PREVIEW_VISUALIZATION:
                response, error_message = await self._preview_viz_async(request)
            elif action == ActionType.COMMIT_VISUALIZATION:
                response, error_message = await self._commit_viz_async(request)
            return self._serialize_response(200, action, response, error_message)
        except Exception as ex:
            return self._serialize_response(
                400, action, self._generate_error_response(action, request), str(ex)
            )

    async def finalize_async(self):
        await self.client.finalize_async()

    async def _process_question_async(self, request) -> Tuple[Dict, str]:
        question = request["question"]
        await Message.objects.acreate(session=self.session, question=question)

        response = await self.client.process_question_async(question)
        message = await Message.objects.acreate(
            session=self.session,
            message_type=Message.MessageType.RESPONSE,
            question=question,
            sql_query=response.sql_query,
            data=response.data,
            visualization_spec=response.visualization_spec,
            error_message=response.error_message,
        )
        return MessageSerializer(message).data, response.error_message

    async def _process_sql_query_async(self, request) -> Tuple[Dict, str]:
        message_id = request["message_id"]
        message = await Message.objects.aget(id=message_id)

        sql_query = request["sql_query"]
        response = await self.client.process_query_async(sql_query=sql_query)
        message.sql_query = response.sql_query
        message.data = response.data
        message.visualization_spec = response.visualization_spec
        message.error_message = response.error_message
        await message.asave()
        return MessageSerializer(message).data, response.error_message

    async def _update_viz_spec_async(self, request) -> Tuple[Dict, str]:
        message_id = request["message_id"]
        message = await Message.objects.aget(id=message_id)
        viz_spec = parse_spec(request["visualization_spec"])

        response = await self.client.process_viz_spec_async(viz_spec)
        message.sql_query = response.sql_query
        message.data = response.data
        message.visualization_spec = response.visualization_spec
        message.error_message = response.error_message
        await message.asave()
        return MessageSerializer(message).data, response.error_message

    async def _generate_report_async(self) -> Tuple[Dict, str]:
        report = await self.client.generate_report_async()
        for question, response in report.items():
            if response.error_message:
                continue
            viz = await Visualization.objects.acreate(
                session=self.session,
                title=question,
                question=question,
                sql_query=response.sql_query,
                data=response.data,
                visualization_spec=response.visualization_spec,
                error_message=response.error_message,
            )
            self.client.add_new_viz_to_report(viz)
        return {}, ""

    async def _add_new_viz_async(self, request) -> Tuple[Dict, str]:
        message_id = request["message_id"]
        message = await Message.objects.aget(id=message_id)
        viz = await Visualization.objects.acreate(
            session=self.session,
            title=message.question,
            question=message.question,
            sql_query=message.sql_query,
            data=message.data,
            visualization_spec=message.visualization_spec,
            error_message=message.error_message,
        )
        self.client.add_new_viz_to_report(viz)
        return {}, ""

    async def _remove_viz_async(self, request) -> Tuple[Dict, str]:
        viz_id = request["viz_id"]
        viz = await Visualization.objects.aget(id=viz_id)
        await viz.adelete()
        self.client.remove_viz_from_report(viz_id)
        return {}, ""

    async def _preview_viz_async(self, request) -> Tuple[Dict, str]:
        viz_id = request["viz_id"]
        viz = await Visualization.objects.aget(id=viz_id)
        viz_spec = parse_spec(request["visualization_spec"])

        response = await self.client.process_viz_spec_async(viz_spec)
        viz.sql_query = response.sql_query
        viz.data = response.data
        viz.visualization_spec = response.visualization_spec
        viz.error_message = response.error_message

        return VisualizationSerializer(viz).data, response.error_message

    async def _commit_viz_async(self, request) -> Tuple[Dict, str]:
        viz_id = request["viz_id"]
        viz = await Visualization.objects.aget(id=viz_id)

        request_viz = json.loads(request["viz"])
        viz.title = request_viz["title"]
        viz.sql_query = request_viz["sql_query"]
        viz.data = request_viz["data"]
        viz.visualization_spec = request_viz["visualization_spec"]
        viz.error_message = request_viz["error_message"]
        await viz.asave()
        return {}, ""

    def _validate_request(self, request):
        action = request.get("action", "")
        if action not in ActionType:
            raise ValueError("Undefined action received")
        ActionType.validate(action, request)

    def _generate_error_response(self, action: str, request: Dict) -> Dict:
        response = {}
        if (
            action == ActionType.PROCESS_SQL_QUERY
            or action == ActionType.UPDATE_VIZ_SPEC
        ):
            response["id"] = request["message_id"]
        return response

    def _serialize_response(
        self, status: int, action: str, response: Dict, error_message: str
    ) -> str:
        return json.dumps(
            {
                "status": status,
                "action": action,
                "data": response,
                "error_message": error_message,
            },
            cls=DjangoJSONEncoder,
        )


class ActionTypeMeta(enum.EnumMeta):
    """
    EnumMeta class to support "in" operator for ActionType.
    """

    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        else:
            return True


class ActionType(str, enum.Enum, metaclass=ActionTypeMeta):
    """
    Enum class for a set of supported actions.
    """

    PROCESS_QUESTION = "process_question"
    PROCESS_SQL_QUERY = "process_sql_query"
    UPDATE_VIZ_SPEC = "update_visualization_spec"
    GENERATE_REPORT = "generate_report"
    ADD_VISUALIZATION = "add_viz"
    REMOVE_VISUALIZATION = "remove_viz"
    PREVIEW_VISUALIZATION = "preview_viz"
    COMMIT_VISUALIZATION = "commit_viz"

    @staticmethod
    def validate(action, request):
        for required_field in ActionType.get_required_fields(action):
            if required_field not in request:
                raise ValueError(f"Invalid request for {action}")

    @staticmethod
    def get_required_fields(action):
        required_fields = []
        if action == ActionType.PROCESS_QUESTION:
            required_fields.extend(["question"])
        elif action == ActionType.PROCESS_SQL_QUERY:
            required_fields.extend(["message_id", "sql_query"])
        elif action == ActionType.UPDATE_VIZ_SPEC:
            required_fields.extend(["message_id", "visualization_spec"])
        elif action == ActionType.ADD_VISUALIZATION:
            required_fields.extend(["message_id"])
        elif action == ActionType.REMOVE_VISUALIZATION:
            required_fields.extend(["viz_id"])
        elif action == ActionType.PREVIEW_VISUALIZATION:
            required_fields.extend(["viz_id", "visualization_spec"])
        elif action == ActionType.COMMIT_VISUALIZATION:
            required_fields.extend(["viz_id", "viz"])
        return required_fields
