from src.transactions import Comment, Post, Follow, Unfollow

class Test_Transactions():
    def test_Comment(self):
        comment = Comment(1, "Joe", "Hello")
        assert comment.execute() == ({"comment_id":1, "user_id":"Joe", "comment":"Hello"}, 0)
        assert comment.execute() == ({"user_id":"Joe"}, 1)

    def test_Post(self):
        post = Post(1, "Joe", "I had a nice day today")
        assert post.execute() == ({"post_id":1, "user_id":"Joe", "content":"I had a nice day today"},0)
        assert post.execute() == ({"user_id": "Joe"},1)
    
    def test_Follow(self):
        follow = Follow("Joe", "Bob")
        assert follow.execute() == ({"edge_id": "Joe-Bob"},0)
        assert follow.execute() == ({"user_id":"Joe","follower_id":"Bob"},1)
        assert follow.execute() == ({"user_id":"Joe"},2)

    def test_Unfollow(self):
        unfollow = Unfollow("Joe", "Bob")
        assert unfollow.execute() == ({"edge_id": "Joe-Bob"},0)
        assert unfollow.execute() == ({"user_id":"Joe","follower_id":"Bob"},1)
        assert unfollow.execute() == ({"user_id":"Joe"},2)