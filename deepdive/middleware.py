import timeit
import logging


logger = logging.getLogger(__name__)


class RequestMetricsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start = timeit.default_timer()
        response = self.get_response(request)
        end = timeit.default_timer()

        logger.warn(f"{request} [{str(end - start)} ms]")
        return response
