## This file defines dynamic transactions that were not included in the SC-Graph 

import timeit
from abc import ABC, abstractmethod
from overrides import override
from src.transactions import Transaction

# Naive deletion of post
class DeletePost(Transaction):
    
    @override
    def __init__(self, post_id, user_id, username):
        """ Creates a DeletePost Object which represents a delete post transactions
        that was not defined in the system.
        """
        super().__init__()

        self.post_id = post_id
        self.user_id = user_id
        self.username = username
        self.start = timeit.default_timer()
        self.stop = None
        self.time = None

    @override
    def execute(self):
        match self.hop_number:
            case 0:
                self.hop_number += 1
                return {"post_id": self.post_id}, 0
            case 1:
                return {"user_id": self.user_id}, 1