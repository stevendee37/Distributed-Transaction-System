# This file provides the class for the distributed system.
import os
import pandas as pd

class DistributedSystem():
    def __init__(self, sys_name: str):
        self.sys_name = sys_name
        self.partitions = {}
        self.root = os.path.join(os.path.join(os.getcwd(), "databases"), self.sys_name)
        return
    
    def partitionDatabase(self, file_name, database_class: str):
        """ Partitions database based on alphabetical sorting
        Args:
            file_name: file for unparitioned database
            database_class: database class for one of the predefined values:
                - profile
                - graph
                - posts
                - comments
                - edges
        """
        # Reads from unpartitioned database (i.e. test-profile.csv, test-graph.csv)
        if database not in ["profile", "graph", "posts", "comments", "edges"]:
            exit("Unsupported Database type")
        database_path = os.path.join(self.root, f"{file_name}-{database_class}")
        database = pd.read_csv(database_path)
        
        output_dir = os.path.join(self.root, "partitions")

        # Adds data into appropriate partition file based on parition criteria
        for _, row in database.iterrows():
            username = row['username']
            partition = self.determinePartition(username)
            partition_file = os.path.join(output_dir, f"{file_name}-{database_class}_{partition}.csv")

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