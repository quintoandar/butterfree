"""Module docstring example, following Google's docstring style."""
import logging.config
import os
import sys

sys.path.insert(0, os.path.abspath("."))

logging.config.fileConfig(fname="butterfree/logging.conf")
