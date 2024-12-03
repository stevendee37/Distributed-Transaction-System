import pytest
import os
from src.system import DistributedSystem
from src.server import Server

@pytest.fixture
def system():
    root = os.getcwd()
    return DistributedSystem(root,"test")

@pytest.fixture
def server_1():
    root_dir = os.getcwd()
    sys_name = "test"
    sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
    return Server("test", sys_dir, "1")

@pytest.fixture
def server_2():
    root_dir = os.getcwd()
    sys_name = "test"
    sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
    return Server("test", sys_dir, "2")

@pytest.fixture
def server_3():
    root_dir = os.getcwd()
    sys_name = "test"
    sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
    return Server("test", sys_dir, "3")