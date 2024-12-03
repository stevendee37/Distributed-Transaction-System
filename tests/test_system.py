import os
import pytest
import pandas as pd

@pytest.mark.usefixtures("system")
class TestSystem():

    def test_Init(self, system):
        system_dir = os.path.join(os.path.join(os.getcwd(), "systems"), "test")
        assert system.sys_name == "test"
        assert system.system_dir == system_dir
        assert system.partitions == {}

    def test_PartitionDatabase(self, system):
        output_dir = os.path.join(os.path.join(os.path.join(os.getcwd(), "systems"), "test"), "partitions")
        system.partitionDatabase("profile")

        partition_file = os.path.join(output_dir, "test-profile_1.csv")
        db = pd.read_csv(partition_file)
        first_row = db.iloc[0]
        second_row = db.iloc[1]
        assert first_row["username"] == "Amy"
        assert second_row["username"] == "Brian"
        db = db.drop(index=[0,1])
        db.to_csv(partition_file, index=False)

        partition_file = os.path.join(output_dir, "test-profile_2.csv")
        db = pd.read_csv(partition_file)
        first_row = db.iloc[0]
        assert first_row["username"] == "Michael"
        db = db.drop(index=0)
        db.to_csv(partition_file, index=False)

        partition_file = os.path.join(output_dir, "test-profile_3.csv")
        db = pd.read_csv(partition_file)
        first_row = db.iloc[0]
        assert first_row["username"] == "Steven"
        db = db.drop(index=0)
        db.to_csv(partition_file, index=False)

    def test_DeterminePartition(self, system):
        assert system.determinePartition("Brian") == 1
        assert system.determinePartition("Megan") == 2
        assert system.determinePartition("Tyler") == 3