# This file provides the class for the distributed system.

import os
import threading
import timeit
from src.utils import determinePartition
from src.server import Server
from src.transactions import Comment, Post, Follow, Unfollow
from src.dynamic_transactions import DeletePost

class DistributedSystem():
    def __init__(self, root_dir, sys_name:str):
        self.sys_name = sys_name
        self.sys_dir = os.path.join(os.path.join(root_dir, "systems"), self.sys_name)
        self.transactions = []
        self.dynamic_transactions = []
        self.dynamic_ready = False

        """ Server format:
        Partition 1: Graph, Edges, and Profiles A-H
        Partition 2: Posts and Profiles I-P
        Partition 3: Comments and Profiles Q-Z
        """
        self.servers = {
            "1": Server(self.sys_name, self.sys_dir, "1"),
            "2": Server(self.sys_name, self.sys_dir, "2"),
            "3": Server(self.sys_name, self.sys_dir, "3")
        }

    def loadTransactions(self, transactions:list):
        """ Loads list of transactions for system to process and carry out
        """
        self.transactions = transactions
    
    def processTransactions(self):
        """ Serves as transaction chopping/processing layer
        """
        for transaction in self.transactions:
            match transaction:
                case Post():
                    if transaction.hop_number == 1:
                        partition = determinePartition(transaction.username)
                        self.servers[f"{partition}"].appendHop(transaction)
                    else:
                        self.servers["2"].appendHop(transaction)
                case Comment():
                    if transaction.hop_number == 1:
                        partition = determinePartition(transaction.username)
                        self.servers[f"{partition}"].appendHop(transaction)
                    else:
                        self.servers["3"].appendHop(transaction)
                case Follow():
                    if transaction.hop_number == 2:
                        partition = determinePartition(transaction.username)
                        self.servers[f"{partition}"].appendHop(transaction)
                    else:
                        self.servers["1"].appendHop(transaction)
                case Unfollow():
                    if transaction.hop_number == 2:
                        partition = determinePartition(transaction.username)
                        self.servers[f"{partition}"].appendHop(transaction)
                    else:
                        self.servers["1"].appendHop(transaction)
                case _:
                    if self.dynamic_ready:
                        match transaction:
                            case DeletePost():
                                if transaction.hop_number == 1:
                                    partition = determinePartition(transaction.username)
                                    self.servers[f"{partition}"].appendHop(transaction)
                                else:
                                    self.servers["2"].appendHop(transaction)
                    else:
                        self.dynamic_transactions.append(transaction)
                    
        self.transactions = []

    def retrieveOutgoing(self):
        """ Retrieves outgoing hops from all servers
        """
        self.transactions += self.servers["1"].outgoing_hops
        self.transactions += self.servers["2"].outgoing_hops
        self.transactions += self.servers["3"].outgoing_hops
        self.servers["1"].outgoing_hops = []
        self.servers["2"].outgoing_hops = []
        self.servers["3"].outgoing_hops = []

    def runTransactions(self):
        """ Run loaded Transactions
        """
        start = timeit.default_timer()
        while True:
            if len(self.transactions) == 0:
                break
            self.processTransactions()

            threads = []
            """ Threading implemented, each server runs on a different thread
            """
            for server_id in ["1", "2", "3"]:
                thread = threading.Thread(target=self.servers[server_id].runHops())
                threads.append(thread)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            """ No threading, each server runs as a single server
            """
            # self.servers["1"].runHops()
            # self.servers["2"].runHops()
            # self.servers["3"].runHops()

            self.retrieveOutgoing()
        
        """ Run dynamic transactions
        
        """
        self.transactions = self.dynamic_transactions
        self.dynamic_transactions = []
        self.dynamic_ready = True
        while True:
            if len(self.transactions) == 0:
                break
            self.processTransactions()
            threads = []
            for server_id in ["1", "2", "3"]:
                thread = threading.Thread(target=self.servers[server_id].runHops())
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()

            self.retrieveOutgoing()

        output_dir = os.path.join(self.sys_dir,'result')
        for i in range(1,4):
            server = self.servers[f"{i}"]
            match i:
                case 1:
                    output_file = os.path.join(output_dir, f"{self.sys_name}-graph.csv")
                    server.graph.to_csv(output_file, index=False, header=True)
                    output_file = os.path.join(output_dir, f"{self.sys_name}-edges.csv")
                    server.edges.to_csv(output_file, index=False, header=True)
                    output_file = os.path.join(output_dir, f"{self.sys_name}-profile_{i}.csv")
                    server.profile.to_csv(output_file, index=False, header=True)
                case 2:
                    output_file = os.path.join(output_dir, f"{self.sys_name}-posts.csv")
                    server.posts.to_csv(output_file, index=False, header=True)
                    output_file = os.path.join(output_dir, f"{self.sys_name}-profile_{i}.csv")
                    server.profile.to_csv(output_file, index=False, header=True)
                case 3:
                    output_file = os.path.join(output_dir, f"{self.sys_name}-comments.csv")
                    server.comments.to_csv(output_file, index=False, header=True)
                    output_file = os.path.join(output_dir, f"{self.sys_name}-profile_{i}.csv")
                    server.profile.to_csv(output_file, index=False, header=True)
        stop = timeit.default_timer()
        time = f"Time: {stop-start}"
        print(time)