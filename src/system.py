# This file provides the class for the distributed system.

import os
import pandas as pd
import threading
from src.utils import determinePartition
from src.server import Server
from src.transactions import Comment, Post, Follow, Unfollow

class DistributedSystem():
    def __init__(self, root_dir, sys_name:str):
        self.sys_name = sys_name
        self.sys_dir = os.path.join(os.path.join(root_dir, "systems"), self.sys_name)
        self.transactions = []

        """Server format:
        Partition 1: Graph, Edges, and Profiles A-H
        Partition 2: Posts and Profiles I-P
        Partition 3: Comments and Profiles Q-Z
        """
        self.servers = {
            "1": Server(self.sys_name, self.sys_dir, "1"),
            "2": Server(self.sys_name, self.sys_dir, "2"),
            "3": Server(self.sys_name, self.sys_dir, "3")
        }

    def addTransaction(self, transaction):
        self.transactions.append(transaction)

    def loadTransactions(self, transactions:list):
        """ Loads list of transactions for system to process and carry out
        """
        self.transactions += transactions
    
    def processTransactions(self):
        """ Serves as transaction chopping/processing layer
        """
        for transaction in self.transactions:
            match transaction:
                case Post():
                    self.servers["2"].appendHop(transaction)
                case Comment():
                    self.servers["3"].appendHop(transaction)
                case Follow():
                    self.servers["1"].appendHop(transaction)
                case Unfollow():
                    self.servers["1"].appendHop(transaction)
                case _:
                    exit("Unsupported transaction type")
        self.transactions = []

    # def partitionDatabase(self, db_class:str):
    #     """ Partitions database based on alphabetical sorting
    #     Args:
    #         database_class: database class for one of the predefined values:
    #             - profile
    #             - graph
    #             - posts
    #             - comments
    #             - edges
    #     """
    #     # Reads from unpartitioned database (i.e. test-profile.csv, test-graph.csv)
    #     if db_class not in ["profile", "graph", "posts", "comments", "edges"]:
    #         exit("Unsupported Database type")
    #     db_path = os.path.join(self.sys_dir, f"{self.sys_name}-{db_class}.csv")
    #     db = pd.read_csv(db_path)
        
    #     output_dir = os.path.join(self.sys_dir, "partitions")

    #     # Adds data into appropriate partition file based on parition criteria
    #     for _, row in db.iterrows():
    #         username = row['username']
    #         partition = determinePartition(username)
    #         partition_file = os.path.join(output_dir, f"{self.sys_name}-{db_class}_{partition}.csv")

    #         pd.DataFrame([row]).to_csv(partition_file, mode='a', index=False, header=not os.path.exists(partition_file))