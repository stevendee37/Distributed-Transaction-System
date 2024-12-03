from src.transactions import Comment, Post, Follow, Unfollow

class Test_Transactions():
    def test_Comment(self):
        comment = Comment(1, "Joe", "Hello")
        assert comment.execute() == [1, "Joe", "Hello"]
        assert comment.execute() == ["Joe"]

    def test_Post(self):
        post = Post(1, "Joe", "I had a nice day today")
        assert post.execute() == [1, "Joe", "I had a nice day today"]
        assert post.execute() == ["Joe"]
    
    def test_Follow(self):
        follow = Follow("Joe", "Bob")
        assert follow.execute() == ["Joe-Bob"]
        assert follow.execute() == ["Joe", "Bob"]
        assert follow.execute() == ["Joe"]

    def test_Unfollow(self):
        unfollow = Unfollow("Joe", "Bob")
        assert unfollow.execute() == ["Joe-Bob"]
        assert unfollow.execute() == ["Joe", "Bob"]
        assert unfollow.execute() == ["Joe"]