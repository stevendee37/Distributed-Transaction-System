## This file implements the predefined transactions for the system
from abc import ABC, abstractmethod
from overrides import override

class Transaction(ABC):
    @abstractmethod
    def __init__(self) -> None:
        self.hop_number = 0

    @abstractmethod
    def execute(self):
        pass

    def nextHop(self):
        self.hop_number += 1

class Post(Transaction):
    @override
    def __init__(self, post_id, username, content):
        """ Creates a Post object which represents a post transaction
        Args:
            - hop_number: describes which hop the transaction is currently on
        """
        super().__init__()

        self.post_id = post_id
        self.username = username
        self.content = content
    @override
    def execute(self):
        match self.hop_number:
            case 0:
                self.hop_number += 1
                return {"post_id":self.post_id, "user_id":self.username, "content":self.content}, 0
            case 1:
                return {"user_id":self.username}, 1

class Comment(Transaction):
    @override
    def __init__(self, comment_id, username, comment):
        """ Creates a Comment object which represents a comment transaction
        Args:
            - comment_id: unique ID of comment that is being created
            - comment: content of written comment
        """
        super().__init__()

        self.comment_id = comment_id
        self.username = username
        self.comment = comment

    @override
    def execute(self):
        match self.hop_number:
            case 0:
                self.hop_number += 1
                return {"comment_id":self.comment_id, "user_id":self.username, "comment":self.comment}, 0
            case 1:
                return {"user_id": self.username}, 1
            
class Follow(Transaction):
    @override
    def __init__(self, graph_id, username, follower):
        """ Creates a Follow object which represents a follow transaction
        Args:
            - graph_id: unique ID of edge in graph that represents a follower relationship
            - user_id: unique ID of user who is being followed
            - username: name of user who is being followed
            - follower: name of follower who is following user
        """
        super().__init__()

        self.graph_id = graph_id
        self.username = username
        self.follower = follower
    
    @override
    def execute(self):
        match self.hop_number:
            case 0:
                self.hop_number += 1
                # Return unique edgeID to identify if edge already exists in table
                return {"edge_id":f"{self.username}-{self.follower}"}, 0
            case 1:
                self.hop_number += 1
                return {"graph_id":self.graph_id, "user_id":self.username, "follower_id":self.follower}, 1
            case 2:
                return {"user_id": self.username}, 2

class Unfollow(Transaction):
    @override
    def __init__(self, graph_id, username, follower):
        """ Creates an Unfollow object which represents an unfollow transaction
        """
        super().__init__()

        self.graph_id = graph_id
        self.username = username
        self.follower = follower
    
    @override
    def execute(self):
        match self.hop_number:
            case 0:
                self.hop_number += 1
                return {"edge_id":f"{self.username}-{self.follower}"}, 0
            case 1:
                self.hop_number += 1
                return {"graph_id":self.graph_id, "user_id":self.username, "follower_id":self.follower}, 1
            case 2:
                self.hop_number += 1
                return {"user_id": self.username}, 2