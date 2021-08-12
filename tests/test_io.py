import logging
from pathlib import Path
import re
import sys

from click.testing import CliRunner
import datacube
import pytest

import dea_conflux.io as io
from .constants import *

logging.basicConfig(level=logging.INFO)


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []
