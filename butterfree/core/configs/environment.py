"""Holds functions for managing the running environment."""

import os

import git
import yaml

DEVELOPMENT = "dev"
HOMOLOG = "forno"
STAGING = "staging"
PRODUCTION = "prod"
_ENVIRONMENT_SPECIFICATION_FILENAME = "environment.yaml"


def _is_project_root(path: str) -> bool:
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False


def _sanitize_spec_entry(entry):
    if entry is None:
        return entry

    if not isinstance(entry, dict):
        return str(entry)

    mapping = "map"
    for key, value in entry.items():
        mapping += f" {key}:{_sanitize_spec_entry(value)}"
    return mapping.replace(" ", "[", 1) + "]"


def get_environment_specification(filename: str = None) -> dict:
    """Gets environment specification from a default file."""
    spec_filepath = None
    search_path = __file__
    filename = filename or _ENVIRONMENT_SPECIFICATION_FILENAME
    while not spec_filepath:
        search_path = os.path.dirname(search_path)
        try:
            spec_filepath = next(
                entry.path
                for entry in os.scandir(search_path)
                if entry.is_file() and entry.name == filename
            )
        except StopIteration:
            if _is_project_root(search_path):
                raise RuntimeError(f"{_ENVIRONMENT_SPECIFICATION_FILENAME} not found")

    with open(spec_filepath, "r") as stream:
        spec_dict = yaml.safe_load(stream)

    sanitized_spec = {}
    for key, value in spec_dict.items():
        sanitized_spec[key] = _sanitize_spec_entry(value)
    return sanitized_spec


specification = get_environment_specification()


def get_current_environment() -> str:
    """Gets current environment tag.

     It is expected to assume one of these values: "dev", "forno", "staging" or "prod".
     "dev" is the default.

    :return: the value of the "ENVIRONMENT" environment variable.
    """
    return get_variable("ENVIRONMENT", DEVELOPMENT)


def is_development() -> bool:
    """Checks whether the running environment tag refers to a development env or not.

    :return: boolean value.
    """
    return get_current_environment() == DEVELOPMENT


def is_homolog() -> bool:
    """Checks whether the running environment is an homologation env or not.

    :return: boolean value.
    """
    return get_current_environment() == HOMOLOG


def is_staging() -> bool:
    """Checks whether the running environment is a staging env or not.

    :return: boolean value.
    """
    return get_current_environment() == STAGING


def is_production() -> bool:
    """Checks whether the running environment is a production env or not.

    :return: boolean value indicating
    """
    return get_current_environment() == PRODUCTION


class UnspecifiedVariableError(RuntimeError):
    """Environment variables not set error."""

    def __init__(self, variable_name: str):
        """Initialize error object for a single env variable.

        :param variable_name: env variable name.
        """
        super().__init__(
            f'Variable "{variable_name}" is not listed in the environment'
            f' specification\nUpdate the "{_ENVIRONMENT_SPECIFICATION_FILENAME}" file'
            f' to include "{variable_name}"'
        )


def get_variable(variable_name: str, default_value: str = None) -> str:
    """Gets an environment variable.

    The variable comes from it's explicitly declared value in the running environment or
    from the default value declared in the environment.yaml specification or from the
    :param default_value:.

    :param variable_name: environment variable name
    :param default_value: default value to use in case no value is set in the
        environment nor in the environment.yaml specification file
    :return: the variable's string value
    """
    try:
        spec_default = specification[variable_name]
    except KeyError:
        raise UnspecifiedVariableError(variable_name)
    return os.getenv(variable_name) or spec_default or default_value


def describe_variable(variable_name: str, *, bash_formatting: bool = True) -> str:
    """Describes an environment variable.

    The description will state the variable's name, current value and origin, where the
    origin may be the running environment or the specification default.

    :param variable_name: environment variable name
    :param bash_formatting: boolean indicating whether or not to use bash formatting
        markups.
    :return: a string describing the requested environment variable in human-readable
        format.
    """
    set_bold = "\033[1m" if bash_formatting else ""
    unset_bold = "\033[0m" if bash_formatting else ""
    try:
        spec_default = specification[variable_name]
    except KeyError:
        raise UnspecifiedVariableError(variable_name)
    value_from_env = os.getenv(variable_name)
    if value_from_env:
        variable_value = value_from_env
        variable_origin = "running environment"
    else:
        variable_value = spec_default
        variable_origin = (
            f"default value from the"
            f' "{_ENVIRONMENT_SPECIFICATION_FILENAME}" specification'
        )
    return (
        f"{set_bold}{variable_name}{unset_bold}: {variable_value}"
        f" (set by the {variable_origin})"
    )


def describe_environment(*, bash_formatting: bool = True) -> str:
    """Describes the current running environment.

    :param bash_formatting: boolean indicating whether or not to use bash formatting
        markups.
    :return: a multiline string describing the current running environment in
        human-readable format.
    """
    set_bold = "\033[1m" if bash_formatting else ""
    unset_bold = "\033[0m" if bash_formatting else ""
    environment_description = (
        f"\n{set_bold}Environment Variables\n====================={unset_bold}\n"
    )
    for variable in specification.keys():
        variable_description = describe_variable(
            variable, bash_formatting=bash_formatting
        )
        environment_description += f"{variable_description}\n"
    return environment_description


if __name__ == "__main__":
    print(describe_environment())
