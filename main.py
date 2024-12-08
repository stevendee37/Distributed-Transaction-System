import os
from src.system import DistributedSystem
from src.transactions import Post, Comment, Follow, Unfollow
from src.dynamic_transactions import DeletePost
from src.utils import getTransactions
# transactions = [Post(1,1,"Aaron","hello")]
if __name__ == "__main__":
    system = DistributedSystem(os.getcwd(),"medium")
    transactions = getTransactions("transactions_medium")
    system.loadTransactions(transactions)
    system.runTransactions()