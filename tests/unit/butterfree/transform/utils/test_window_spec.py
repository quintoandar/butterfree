from butterfree.transform.utils.window_spec import Window


class TestWindow:
    def test_build_metadata(self):
        # given
        window = Window(
            partition_by="id",
            order_by="timestamp",
            mode="fixed_windows",
            window_definition="2 hours",
        )

        # when
        metadata = window.build_metadata()

        # then
        assert metadata == "2 hours"
