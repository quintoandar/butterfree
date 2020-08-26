import pytest
from pyspark.sql.functions import expr

from butterfree.hooks import Hook, HookableComponent
from butterfree.testing.dataframe import assert_dataframe_equality


class TestComponent(HookableComponent):
    def construct(self, dataframe):
        pre_hook_df = self.run_pre_hooks(dataframe)
        construc_df = pre_hook_df.withColumn("feature", expr("feature * feature"))
        return self.run_post_hooks(construc_df)


class AddHook(Hook):
    def __init__(self, value):
        self.value = value

    def run(self, dataframe):
        return dataframe.withColumn("feature", expr(f"feature + {self.value}"))


class TestHookableComponent:
    def test_add_hooks(self):
        # arrange
        hook1 = AddHook(value=1)
        hook2 = AddHook(value=2)
        hook3 = AddHook(value=3)
        hook4 = AddHook(value=4)
        hookable_component = HookableComponent()

        # act
        hookable_component.add_pre_hook(hook1, hook2)
        hookable_component.add_post_hook(hook3, hook4)

        # assert
        assert hookable_component.pre_hooks == [hook1, hook2]
        assert hookable_component.post_hooks == [hook3, hook4]

    @pytest.mark.parametrize(
        "enable_pre_hooks, enable_post_hooks",
        [("not boolean", False), (False, "not boolean")],
    )
    def test_invalid_enable_hook(self, enable_pre_hooks, enable_post_hooks):
        # arrange
        hookable_component = HookableComponent()

        # act and assert
        with pytest.raises(ValueError):
            hookable_component.enable_pre_hooks = enable_pre_hooks
            hookable_component.enable_post_hooks = enable_post_hooks

    @pytest.mark.parametrize(
        "pre_hooks, post_hooks",
        [
            ([AddHook(1)], "not a list of hooks"),
            ([AddHook(1)], [AddHook(1), 2, 3]),
            ("not a list of hooks", [AddHook(1)]),
            ([AddHook(1), 2, 3], [AddHook(1)]),
        ],
    )
    def test_invalid_hooks(self, pre_hooks, post_hooks):
        # arrange
        hookable_component = HookableComponent()

        # act and assert
        with pytest.raises(ValueError):
            hookable_component.pre_hooks = pre_hooks
            hookable_component.post_hooks = post_hooks

    @pytest.mark.parametrize(
        "pre_hook, enable_pre_hooks, post_hook, enable_post_hooks",
        [
            (AddHook(value=1), False, AddHook(value=1), True),
            (AddHook(value=1), True, AddHook(value=1), False),
            ("not a pre-hook", True, AddHook(value=1), True),
            (AddHook(value=1), True, "not a pre-hook", True),
        ],
    )
    def test_add_invalid_hooks(
        self, pre_hook, enable_pre_hooks, post_hook, enable_post_hooks
    ):
        # arrange
        hookable_component = HookableComponent()
        hookable_component.enable_pre_hooks = enable_pre_hooks
        hookable_component.enable_post_hooks = enable_post_hooks

        # act and assert
        with pytest.raises(ValueError):
            hookable_component.add_pre_hook(pre_hook)
            hookable_component.add_post_hook(post_hook)

    def test_run_hooks(self, spark_session):
        # arrange
        input_dataframe = spark_session.sql("select 2 as feature")
        test_component = (
            TestComponent()
            .add_pre_hook(AddHook(value=1))
            .add_post_hook(AddHook(value=1))
        )
        target_table = spark_session.sql("select 10 as feature")

        # act
        output_df = test_component.construct(input_dataframe)

        # assert
        assert_dataframe_equality(output_df, target_table)
