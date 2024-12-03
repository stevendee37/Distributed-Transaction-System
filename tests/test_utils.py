import src.utils as utils

class TestUtils():
    def test_DeterminePartition(self):
        assert utils.determinePartition("Brian") == 1
        assert utils.determinePartition("Megan") == 2
        assert utils.determinePartition("Tyler") == 3