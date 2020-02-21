import re

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


def test_get_current_environment_default(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", "overwrite")
    monkeypatch.delenv("ENVIRONMENT")

    # when
    current_environment = environment.get_current_environment()

    # then
    assert current_environment == environment.DEVELOPMENT


def test_get_current_environment_explicit(monkeypatch):
    # given
    expected_environment = "DUMMY"
    monkeypatch.setenv("ENVIRONMENT", expected_environment)

    # when
    current_environment = environment.get_current_environment()

    # then
    assert current_environment == expected_environment


def test_is_development_in_development(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.DEVELOPMENT)

    # when
    is_development = environment.is_development()

    # then
    assert is_development is True


def test_is_development_in_homolog(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.HOMOLOG)

    # when
    is_development = environment.is_development()

    # then
    assert is_development is False


def test_is_development_in_staging(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.STAGING)

    # when
    is_development = environment.is_development()

    # then
    assert is_development is False


def test_is_development_in_production(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.PRODUCTION)

    # when
    is_development = environment.is_development()

    # then
    assert is_development is False


def test_is_development_in_dummy(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", "DUMMY")

    # when
    is_development = environment.is_development()

    # then
    assert is_development is False


def test_is_homolog_in_development(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.DEVELOPMENT)

    # when
    is_homolog = environment.is_homolog()

    # then
    assert is_homolog is False


def test_is_homolog_in_homolog(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.HOMOLOG)

    # when
    is_homolog = environment.is_homolog()

    # then
    assert is_homolog is True


def test_is_homolog_in_staging(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.STAGING)

    # when
    is_homolog = environment.is_homolog()

    # then
    assert is_homolog is False


def test_is_homolog_in_production(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.PRODUCTION)

    # when
    is_homolog = environment.is_homolog()

    # then
    assert is_homolog is False


def test_is_homolog_in_dummy(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", "DUMMY")

    # when
    is_homolog = environment.is_homolog()

    # then
    assert is_homolog is False


def test_is_staging_in_development(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.DEVELOPMENT)

    # when
    is_staging = environment.is_staging()

    # then
    assert is_staging is False


def test_is_staging_in_homolog(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.HOMOLOG)

    # when
    is_staging = environment.is_staging()

    # then
    assert is_staging is False


def test_is_staging_in_staging(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.STAGING)

    # when
    is_staging = environment.is_staging()

    # then
    assert is_staging is True


def test_is_staging_in_production(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.PRODUCTION)

    # when
    is_staging = environment.is_staging()

    # then
    assert is_staging is False


def test_is_staging_in_dummy(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", "DUMMY")

    # when
    is_staging = environment.is_staging()

    # then
    assert is_staging is False


def test_is_production_in_development(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.DEVELOPMENT)

    # when
    is_production = environment.is_production()

    # then
    assert is_production is False


def test_is_production_in_homolog(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.HOMOLOG)

    # when
    is_production = environment.is_production()

    # then
    assert is_production is False


def test_is_production_in_staging(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.STAGING)

    # when
    is_production = environment.is_production()

    # then
    assert is_production is False


def test_is_production_in_production(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", environment.PRODUCTION)

    # when
    is_production = environment.is_production()

    # then
    assert is_production is True


def test_is_production_in_dummy(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", "DUMMY")

    # when
    is_production = environment.is_production()

    # then
    assert is_production is False


def test_get_environment_specification(mocker):
    # given
    mocker.patch(
        "butterfree.core.configs.environment.next",
        return_value="tests/unit/butterfree/core/configs/test_env.yaml",
    )
    # when
    specs = environment.get_environment_specification()
    # then
    specs["ENVIRONMENT"] = "test"


def test_get_environment_specificatio_error(mocker):
    # given
    mocker.patch(
        "butterfree.core.configs.environment._is_project_root", return_value=True
    )
    # then
    with pytest.raises(RuntimeError, match="not found"):
        _ = environment.get_environment_specification()


def test_describe_variable(monkeypatch):
    # when
    description = environment.describe_variable("ENVIRONMENT")

    # then
    assert (
        re.fullmatch(
            r".*ENVIRONMENT.*dev.*set by the default value from the.*specification.*",
            description,
        )
        is not None
    )


def test_describe_variable_by_environment(monkeypatch):
    # given
    monkeypatch.setenv("ENVIRONMENT", "LUL")

    # when
    description = environment.describe_variable("ENVIRONMENT")

    # then
    assert (
        re.fullmatch(
            r".*ENVIRONMENT.*LUL.*set by the running environment.*", description
        )
        is not None
    )


def test_describe_environment(mocker):
    # given
    mocker.patch.dict(
        environment.specification, {"ENVIRONMENT": "test"},
    )

    # when
    description = environment.describe_environment()

    # then
    assert (
        re.match(
            r"\n.*Environment Variables\n.*\n.*ENVIRONMENT.*test.*set"
            r" by the default value from the.*specification\)\n",
            description,
        )
        is not None
    )
