import os
from src.transactions import Post, Comment, Follow, Unfollow
from src.dynamic_transactions import DeletePost

def determinePartition(username: str):
    """ Determines dataset partition to send table entry to.
    Sorts based on alphabetical order.
    """
    if username < "I":
        return 1
    elif username >= "I" and username < "Q":
        return 2
    else:
        return 3
        
def getTransactions(filename):
    """ Gets transactions from .txt file
    """
    root = os.getcwd()
    file_path = os.path.join(os.path.join(root,"transactions"), f"{filename}.txt")
    transactions = []
    with open(file_path, 'r') as file:
        for line in file:
            line = line.rstrip('\n')
            transaction = line.split(',')
            match transaction[0]:
                case "Comment":
                    transactions.append(Comment(int(transaction[1]),int(transaction[2]),transaction[3],transaction[4]))
                case "Post":
                    transactions.append(Post(int(transaction[1]),int(transaction[2]),transaction[3],transaction[4]))
                case "Follow":
                    transactions.append(Follow(int(transaction[1]),int(transaction[2]),transaction[3],transaction[4]))
                case "Unfollow":
                    transactions.append(Unfollow(int(transaction[1]),int(transaction[2]),transaction[3],transaction[4]))
                case "DeletePost":
                    transactions.append(DeletePost(int(transaction[1]),int(transaction[2]),transaction[3]))
    return transactions