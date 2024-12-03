from src.transactions import Comment, Post, Follow, Unfollow

class Test_Transactions():
    def test_Comment(self):
        comment = Comment(1, "Joe", "Hello")
        assert comment.execute() == ([1, "Joe", "Hello"], 0)
        assert comment.execute() == (["Joe"], 1)

    def test_Post(self):
        post = Post(1, "Joe", "I had a nice day today")
        assert post.execute() == ([1, "Joe", "I had a nice day today"],0)
        assert post.execute() == (["Joe"],1)
    
    def test_Follow(self):
        follow = Follow("Joe", "Bob")
        assert follow.execute() == (["Joe-Bob"],0)
        assert follow.execute() == (["Joe", "Bob"],1)
        assert follow.execute() == (["Joe"],2)

    def test_Unfollow(self):
        unfollow = Unfollow("Joe", "Bob")
        assert unfollow.execute() == (["Joe-Bob"],0)
        assert unfollow.execute() == (["Joe", "Bob"],1)
        assert unfollow.execute() == (["Joe"],2)