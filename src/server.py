import os
import pandas as pd
from src.transactions import Comment, Post, Follow, Unfollow
from src.dynamic_transactions import DeletePost

class Server():
    def __init__(self, sys_name, sys_dir, partition_num):
        self.sys_dir = sys_dir
        self.sys_name = sys_name
        self.partition_num = partition_num

        self.hops = []
        # TODO: sort by time when hops are added back to system for processing
        self.outgoing_hops = []

        """ Server format:
        Partition 1: Graph, Edges, and Profiles A-H
        Partition 2: Posts and Profiles I-P
        Partition 3: Comments and Profiles Q-Z
        """
        match partition_num:
            case "1":
                self.graph = pd.read_csv(os.path.join(self.sys_dir, f"{self.sys_name}-graph.csv"))
                self.edges = pd.read_csv(os.path.join(self.sys_dir, f"{self.sys_name}-edges.csv"))
            case "2":
                self.posts = pd.read_csv(os.path.join(self.sys_dir, f"{self.sys_name}-posts.csv"))
            case "3":
                self.comments = pd.read_csv(os.path.join(self.sys_dir, f"{self.sys_name}-comments.csv"))
            case _:
                exit("Unsupported Partition Number")

        self.profile = pd.read_csv(os.path.join(self.sys_dir, f"{self.sys_name}-profile_{self.partition_num}.csv"))
    
    def appendHop(self, hop):
        """ Appends hop to list of hops needed to be done
        """
        if type(hop) == list:
            self.hops += hop
        else:
            self.hops.append(hop)
    
    def clearOutgoing(self):
        self.outgoing_hops = []

    # TODO: potentially add threading here?
    def runHops(self):
        for hop in self.hops:
            match hop:
                case Post():
                    data, hop_iter = hop.execute()
                    match hop_iter:
                        case 0:
                            df = pd.DataFrame(data)
                            self.posts = pd.concat([self.posts,df],ignore_index=True)
                            self.outgoing_hops.append(hop)
                        case 1:
                            user_id = data["user_id"]
                            row_index = self.profile.loc[self.profile["user_id"] == user_id].index
                            self.profile.loc[row_index, 'posts'] += 1
                case Comment():
                    data, hop_iter = hop.execute()
                    match hop_iter:
                        case 0:
                            df = pd.DataFrame(data)
                            self.comments = pd.concat([self.comments,df],ignore_index=True)
                            self.outgoing_hops.append(hop)
                        case 1:
                            user_id = data["user_id"]
                            row_index = self.profile[self.profile["user_id"] == user_id].index
                            self.profile.loc[row_index, 'comments'] += 1
                case Follow():
                    data, hop_iter = hop.execute()
                    match hop_iter:
                        case 0: 
                            edge_id = data["edge_id"]
                            if edge_id not in self.edges["edge_id"].values:
                                df = pd.DataFrame(data)
                                self.edges = pd.concat([self.edges, df],ignore_index=True)
                                self.outgoing_hops.append(hop)
                        case 1:
                            df = pd.DataFrame(data)
                            self.graph = pd.concat([self.graph, df],ignore_index=True)
                            self.outgoing_hops.append(hop)
                        case 2:
                            user_id = data["user_id"]
                            row_index = self.profile.loc[self.profile["user_id"] == user_id].index
                            self.profile.loc[row_index, 'followers'] += 1
                case Unfollow():
                    data, hop_iter = hop.execute()
                    match hop_iter:
                        case 0: 
                            edge_id = data["edge_id"]
                            if edge_id in self.edges["edge_id"].values:
                                self.edges = self.edges[self.edges["edge_id"] != edge_id]
                                self.outgoing_hops.append(hop)
                        case 1:
                            graph_id = data["graph_id"]
                            self.graph = self.graph[self.graph["graph_id"] != graph_id]
                            self.outgoing_hops.append(hop)
                        case 2:
                            user_id = data["user_id"]
                            row_index = self.profile.loc[self.profile["user_id"] == user_id].index
                            self.profile.loc[row_index, 'followers'] -= 1
                case _:
                    match hop:
                        case DeletePost():
                            data, hop_ter = hop.execute()
                            match hop_ter:
                                case 0:
                                    post_id = data["post_id"]
                                    self.posts = self.posts[self.posts["post_id"] != post_id]
                                    self.outgoing_hops.append(hop)
                                case 1:
                                    user_id = data["user_id"]
                                    row_index = self.profile.loc[self.profile["user_id"] == user_id].index
                                    self.profile.loc[row_index, 'posts']
        self.hops = []






    