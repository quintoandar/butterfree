from unittest import TestCase
from unittest.mock import patch

import pytest

from butterfree.transform.transformations import TransformComponent


class TestTransformComponent(TestCase):
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            TransformComponent()

    @patch.multiple(TransformComponent, __abstractmethods__=set())
    def test_parent(self):
        with pytest.raises(TypeError):
            feature_component = TransformComponent()
            feature_component.parent()
