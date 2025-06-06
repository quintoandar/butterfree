import pytest

from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
from butterfree.metadata.writer_metadata import WriterMetadata

writer_param = [
    (HistoricalFeatureStoreWriter(), "HistoricalFeatureStoreWriter"),
    (OnlineFeatureStoreWriter(), "OnlineFeatureStoreWriter"),
]


class TestWriterMetadata:
    @pytest.mark.parametrize("writer, writer_name", writer_param)
    def test_build_metadata(self, writer, writer_name):
        # when
        metadata = writer.build_metadata()

        # then
        assert isinstance(metadata, WriterMetadata)
        assert metadata.type == writer_name
        assert metadata.interval_mode == writer.interval_mode
        assert metadata.write_to_entity == writer.write_to_entity
        assert metadata.db_config == writer.db_config.__class__.__name__
