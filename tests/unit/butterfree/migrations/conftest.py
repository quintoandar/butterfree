from pytest import fixture


@fixture
def dummy_db_schema():
    dummy_db_schema = [
        {"column_name": "id", "type": "INT", "primary_key": True},
        {"column_name": "platform", "type": "STRING", "primary_key": False},
        {"column_name": "ts", "type": "BIGINT", "primary_key": False},
    ]
    return dummy_db_schema


@fixture
def dummy_schema_diff():
    dummy_schema_diff = [
        {"column_name": "kappa_column", "type": "INT", "primary_key": False},
        {"column_name": "pogchamp", "type": "BOOLEAN", "primary_key": False},
    ]
    return dummy_schema_diff


@fixture
def dummy_schema():
    dummy_schema = [
        {"column_name": "id", "type": "INT", "primary_key": True},
        {"column_name": "platform", "type": "STRING", "primary_key": False},
        {"column_name": "ts", "type": "BIGINT", "primary_key": False},
        {"column_name": "kappa_column", "type": "INT", "primary_key": False},
        {"column_name": "pogchamp", "type": "BOOLEAN", "primary_key": False},
    ]
    return dummy_schema
