import unittest

from deepdive.schema import *
from deepdive.viz.processor.viz_type_processor import VizTypeProcessor


class TestVizTypeProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = VizTypeProcessor(None)
