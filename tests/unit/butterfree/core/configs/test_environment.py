import pytest

from butterfree.core.configs import environment


def test_get_variable_success(monkeypatch):
    # given
    specified_variable = "specified_variable"
    effective_value = "effective_value"
    monkeypatch.setenv(specified_variable, effective_value)
    environment.specification[specified_variable] = "spec_default_value"

    # when
    return_value = environment.get_variable(specified_variable, "anything")

    # then
    assert return_value == effective_value


def test_get_variable_from_spec_default(monkeypatch):
    # given
    specified_variable = "specified_variable"
    spec_default_value = "default_value"
    monkeypatch.setenv(specified_variable, "overwrite")
    monkeypatch.delenv(specified_variable)
    environment.specification[specified_variable] = spec_default_value

    # when
    return_value = environment.get_variable(specified_variable, "anything")

    # then
    assert return_value == spec_default_value


def test_get_variable_default(monkeypatch):
    # given
    default = "default_value"
    variable = "environment_variable"
    environment.specification[variable] = None
    monkeypatch.setenv(variable, "overwrite")
    monkeypatch.delenv(variable)

    # when
    return_value = environment.get_variable(variable, default)

    # then
    assert return_value == default


def test_get_variable_out_of_spec_fails(monkeypatch):
    # given
    not_specified_variable = "not_specified_variable"
    monkeypatch.setenv(not_specified_variable, "anything")
    if not_specified_variable in environment.specification:
        del environment.specification[not_specified_variable]

    # then
    with pytest.raises(
        environment.UnspecifiedVariableError, match="not listed in the environment"
    ):
        environment.get_variable(not_specified_variable, "anything")
