import logging
import pytest
from tx.readable_log import getLogger, format_message
from tx.parallex.plasma import start_plasma, stop_plasma

logger = getLogger(__name__, logging.INFO)

@pytest.fixture
def plasma_store():
    p = start_plasma()
    yield p
    stop_plasma(p)
