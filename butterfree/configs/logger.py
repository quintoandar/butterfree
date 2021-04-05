"""Logger funcion."""

import logging


def __config(json_file_logs: bool = False) -> None:

    if json_file_logs:
        return logging.basicConfig(
            format='{"name": "%(name)s", "timestamp": "%(asctime)-15s", '
            '"level": "%(levelname)s", "message": "%(message)s"}',
            level=logging.INFO,
            filename="../logging.json",
        )
    return logging.basicConfig(
        format="%(name)s:%(asctime)-15s:%(levelname)s:< %(message)s >",
        level=logging.INFO,
    )


def __logger(name: str, file_logs: bool = False) -> logging.Logger:

    __config(file_logs)
    return logging.getLogger(name)
