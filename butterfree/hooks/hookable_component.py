"""Definition of hookable component."""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from butterfree.hooks.hook import Hook


class HookableComponent:
    """Defines a component with the ability to hold pre and post hook functions.

    All main module of Butterfree have a common object that enables their integration:
    dataframes. Spark's dataframe is the glue that enables the transmission of data
    between the main modules. Hooks have a simple interface, they are functions that
    accepts a dataframe and outputs a dataframe. These Hooks can be triggered before or
    after the main execution of a component.

    Components from Butterfree that inherit HookableComponent entity, are components
    that can define a series of steps to occur before or after the execution of their
    main functionality.

    Attributes:
        pre_hooks: function steps to trigger before component main functionality.
        post_hooks: function steps to trigger after component main functionality.
        enable_pre_hooks: property to indicate if the component can define pre_hooks.
        enable_post_hooks: property to indicate if the component can define post_hooks.
    """

    def __init__(self) -> None:
        self.pre_hooks = []
        self.post_hooks = []
        self.enable_pre_hooks = True
        self.enable_post_hooks = True

    @property
    def pre_hooks(self) -> List[Hook]:
        """Function steps to trigger before component main functionality."""
        return self.__pre_hook

    @pre_hooks.setter
    def pre_hooks(self, value: List[Hook]) -> None:
        if not isinstance(value, list):
            raise ValueError("pre_hooks should be a list of Hooks.")
        if not all(isinstance(item, Hook) for item in value):
            raise ValueError(
                "All items on pre_hooks list should be an instance of Hook."
            )
        self.__pre_hook = value

    @property
    def post_hooks(self) -> List[Hook]:
        """Function steps to trigger after component main functionality."""
        return self.__post_hook

    @post_hooks.setter
    def post_hooks(self, value: List[Hook]) -> None:
        if not isinstance(value, list):
            raise ValueError("post_hooks should be a list of Hooks.")
        if not all(isinstance(item, Hook) for item in value):
            raise ValueError(
                "All items on post_hooks list should be an instance of Hook."
            )
        self.__post_hook = value

    @property
    def enable_pre_hooks(self) -> bool:
        """Property to indicate if the component can define pre_hooks."""
        return self.__enable_pre_hooks

    @enable_pre_hooks.setter
    def enable_pre_hooks(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("enable_pre_hooks accepts only boolean values.")
        self.__enable_pre_hooks = value

    @property
    def enable_post_hooks(self) -> bool:
        """Property to indicate if the component can define post_hooks."""
        return self.__enable_post_hooks

    @enable_post_hooks.setter
    def enable_post_hooks(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("enable_post_hooks accepts only boolean values.")
        self.__enable_post_hooks = value

    def add_pre_hook(self, *hooks: Hook) -> HookableComponent:
        """Add a pre-hook steps to the component.

        Args:
            hooks: Hook steps to add to pre_hook list.

        Returns:
            Component with the Hook inserted in pre_hook list.

        Raises:
            ValueError: if the component does not accept pre-hooks.
        """
        if not self.enable_pre_hooks:
            raise ValueError("This component does not enable adding pre-hooks")
        self.pre_hooks += list(hooks)
        return self

    def add_post_hook(self, *hooks: Hook) -> HookableComponent:
        """Add a post-hook steps to the component.

        Args:
            hooks: Hook steps to add to post_hook list.

        Returns:
            Component with the Hook inserted in post_hook list.

        Raises:
            ValueError: if the component does not accept post-hooks.
        """
        if not self.enable_post_hooks:
            raise ValueError("This component does not enable adding post-hooks")
        self.post_hooks += list(hooks)
        return self

    def run_pre_hooks(self, dataframe: DataFrame) -> DataFrame:
        """Run all defined pre-hook steps from a given dataframe.

        Args:
            dataframe: data to input in the defined pre-hook steps.

        Returns:
            dataframe after passing for all defined pre-hooks.
        """
        for hook in self.pre_hooks:
            dataframe = hook.run(dataframe)
        return dataframe

    def run_post_hooks(self, dataframe: DataFrame) -> DataFrame:
        """Run all defined post-hook steps from a given dataframe.

        Args:
            dataframe: data to input in the defined post-hook steps.

        Returns:
            dataframe after passing for all defined post-hooks.
        """
        for hook in self.post_hooks:
            dataframe = hook.run(dataframe)
        return dataframe
