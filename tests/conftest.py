import pytest
import os
from src.system import DistributedSystem

@pytest.fixture
def system():
    root = os.getcwd()
    return DistributedSystem(root,"test")