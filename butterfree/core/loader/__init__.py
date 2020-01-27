"""Holds spark data loaders for multiple destinations."""

from butterfree.core.loader.historical_feature_store_loader import (
    HistoricalFeatureStoreLoader,
)
from butterfree.core.loader.online_feature_store_loader import OnlineFeatureStoreLoader

__all__ = ["OnlineFeatureStoreLoader", "HistoricalFeatureStoreLoader"]
