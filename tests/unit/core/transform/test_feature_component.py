from unittest import TestCase
from unittest.mock import Mock, patch

import pytest

from butterfree.core.transform.feature_component import FeatureComponent


class TestFeatureComponent(TestCase):
    def test_cannot_instantiate(self):
        """showing we normally can't instantiate an abstract class"""
        with pytest.raises(TypeError):
            FeatureComponent()

    @patch.multiple(FeatureComponent, __abstractmethods__=set())
    def test_add_method(self):
        with pytest.raises(NotImplementedError):
            feature_component = FeatureComponent()
            component = Mock()
            feature_component.add(component)
