from src.transactions import Comment, Post, Follow, Unfollow

class Test_Transactions():
    def test_Comment(self):
        comment = Comment(1, 1, "Joe", "Hello")
        assert comment.execute() == ({"comment_id":[1], "user_id":[1], "comment":["Hello"]}, 0)
        assert comment.execute() == ({"user_id":1}, 1)

    def test_Post(self):
        post = Post(1, 1, "Joe", "I had a nice day today")
        assert post.execute() == ({"post_id":[1], "user_id":[1], "content":["I had a nice day today"]},0)
        assert post.execute() == ({"user_id": 1},1)
    
    def test_Follow(self):
        follow = Follow(1, 1, "Joe", 2)
        assert follow.execute() == ({"edge_id": ["1-2"]},0)
        assert follow.execute() == ({"graph_id": [1],"user_id":[1],"follower_id":[2]},1)
        assert follow.execute() == ({"user_id":1},2)

    def test_Unfollow(self):
        unfollow = Unfollow(1, 1, "Joe", 2)
        assert unfollow.execute() == ({"edge_id": ["1-2"]},0)
        assert unfollow.execute() == ({"graph_id": 1,"user_id":1,"follower_id":2},1)
        assert unfollow.execute() == ({"user_id":1},2)