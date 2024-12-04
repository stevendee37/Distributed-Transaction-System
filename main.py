import os
from src.system import DistributedSystem
from src.transactions import Post, Comment, Follow, Unfollow
from src.dynamic_transactions import DeletePost

transactions = {
    Post(1,1,"Adam","Woke up early today, feeling good!"),
    Comment(2,1,"Adam","good morning!"),
    Comment(3,7,"Tyler","Cool guy, Tyler"),
    Comment(4,1,"Adam", "Is it your birthday today?"),
    Follow(5,1,"Adam",4),
    Post(6,4,"Joe","I am hungry"),
    DeletePost(1, 1, "Adam"),
    Follow(7,9,"Xavier",2),
    Unfollow(8,3,"Charlie",1),
    Follow(9,9,"Xavier",1),
    Unfollow(10,9,"Xavier",2),
    Comment(11,6,"Natalie","Natalie what are you doing today?")
}

if __name__ == "__main__":
    system = DistributedSystem(os.getcwd(),"small")
    system.loadTransactions(transactions)
    system.runTransactions()