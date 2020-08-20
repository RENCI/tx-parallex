import logging
from multiprocessing import Manager
import pytest
from tx.readable_log import getLogger, format_message
from tx.parallex.objectstore import PlasmaStore, SimpleStore

logger = getLogger(__name__, logging.INFO)

@pytest.fixture
def manager():
    with Manager() as manager:
        yield manager
        
@pytest.fixture(params=[lambda manager: PlasmaStore(manager, 100000), SimpleStore])
def object_store(manager, request):
    p = request.param(manager)
    try:
        p.init()
        yield p
    except:
        p.shutdown()


