# This file provides the class for the distributed system.
import os
import pandas as pd

class DistributedSystem():
    def __init__(self, root_dir, sys_name:str):
        self.sys_name = sys_name
        self.partitions = {}
        self.system_dir = os.path.join(os.path.join(root_dir, "systems"), self.sys_name)
    
    def partitionDatabase(self, db_class:str):
        """ Partitions database based on alphabetical sorting
        Args:
            database_class: database class for one of the predefined values:
                - profile
                - graph
                - posts
                - comments
                - edges
        """
        # Reads from unpartitioned database (i.e. test-profile.csv, test-graph.csv)
        if db_class not in ["profile", "graph", "posts", "comments", "edges"]:
            exit("Unsupported Database type")
        db_path = os.path.join(self.system_dir, f"{self.sys_name}-{db_class}.csv")
        db = pd.read_csv(db_path)
        
        output_dir = os.path.join(self.system_dir, "partitions")

        # Adds data into appropriate partition file based on parition criteria
        for _, row in db.iterrows():
            username = row['username']
            partition = self.determinePartition(username)
            partition_file = os.path.join(output_dir, f"{self.sys_name}-{db_class}_{partition}.csv")

            pd.DataFrame([row]).to_csv(partition_file, mode='a', index=False, header=not os.path.exists(partition_file))

    def determinePartition(self, username: str):
        """ Determines dataset partition to send table entry to.
        Sorts based on alphabetical order.
        """
        if username < "I":
            return 1
        elif username >= "I" and username < "Q":
            return 2
        else:
            return 3