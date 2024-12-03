import os
import pytest
import pandas as pd
from src.server import Server
from src.transactions import Comment, Post, Follow, Unfollow

@pytest.mark.usefixtures("system")
class TestSystem():

    def test_Init(self, system):
        sys_dir = os.path.join(os.path.join(os.getcwd(), "systems"), "test")

        assert system.sys_name == "test"
        assert system.sys_dir == sys_dir
        assert system.transactions == []

        servers = {
            "1": Server("test", sys_dir, "1"),
            "2": Server("test", sys_dir, "2"),
            "3": Server("test", sys_dir, "3")
        }

        assert system.servers["1"].sys_dir == sys_dir
        assert system.servers["2"].sys_dir == sys_dir
        assert system.servers["3"].sys_dir == sys_dir
        assert system.servers["1"].sys_name == "test"
        assert system.servers["2"].sys_name == "test"
        assert system.servers["3"].sys_name == "test"
        assert system.servers["1"].partition_num == "1"
        assert system.servers["2"].partition_num == "2"
        assert system.servers["3"].partition_num == "3"

    def test_LoadTransactions(self, system):
        transaction1 = Comment(1, 1, "Adam", "Hello")
        system.loadTransactions([transaction1])

        assert len(system.transactions) == 1
        assert system.transactions[0] == transaction1

        transaction2 = Post(2, 1, "Adam", "Hello world")
        system.loadTransactions([transaction2])

        assert len(system.transactions) == 2
        assert system.transactions[0] == transaction1
        assert system.transactions[1] == transaction2

    def test_ProcessTransactions(self, system):
        transactions = [
            Comment(1, 1, "Adam", "Hello"), 
            Post(2, 1, "Adam", "Hello world"),
            Follow(1, 1, "Adam", "Bob"),
            Unfollow(1,1, "Adam", "Bob"),
            Comment(3, 1, "Adam", "Goodbye")
        ]

        system.loadTransactions(transactions)

        system.processTransactions()

        assert system.transactions == []

        assert len(system.servers["1"].hops) == 2
        assert len(system.servers["2"].hops) == 1
        assert len(system.servers["3"].hops) == 2

        assert system.servers["1"].hops[0] == transactions[2]
        assert system.servers["1"].hops[1] == transactions[3]
        assert system.servers["2"].hops[0] == transactions[1]
        assert system.servers["3"].hops[0] == transactions[0]
        assert system.servers["3"].hops[1] == transactions[4]
        
    # def test_PartitionDatabase(self, system):
    #     output_dir = os.path.join(os.path.join(os.path.join(os.getcwd(), "systems"), "test"), "partitions")
    #     system.partitionDatabase("profile")

    #     partition_file = os.path.join(output_dir, "test-profile_1.csv")
    #     db = pd.read_csv(partition_file)
    #     first_row = db.iloc[0]
    #     second_row = db.iloc[1]
    #     assert first_row["username"] == "Amy"
    #     assert second_row["username"] == "Brian"
    #     db = db.drop(index=[0,1])
    #     db.to_csv(partition_file, index=False)

    #     partition_file = os.path.join(output_dir, "test-profile_2.csv")
    #     db = pd.read_csv(partition_file)
    #     first_row = db.iloc[0]
    #     assert first_row["username"] == "Michael"
    #     db = db.drop(index=0)
    #     db.to_csv(partition_file, index=False)

    #     partition_file = os.path.join(output_dir, "test-profile_3.csv")
    #     db = pd.read_csv(partition_file)
    #     first_row = db.iloc[0]
    #     assert first_row["username"] == "Steven"
    #     db = db.drop(index=0)
    #     db.to_csv(partition_file, index=False)