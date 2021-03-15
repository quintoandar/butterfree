import logging


def __logger(name: str) -> logging.Logger:
    format_ = "%(name)s:%(asctime)-15s:%(levelname)s:< %(message)s >"
    logging.basicConfig(format=format_, level=logging.INFO)
    return logging.getLogger(name)


cli_logger = __logger("butterfree")
