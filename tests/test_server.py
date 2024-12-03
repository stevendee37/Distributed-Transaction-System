import os
from src.server import Server
from pandas import DataFrame
from src.transactions import Comment, Post, Follow, Unfollow

class TestServer():
    def test_Init1(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "1")
        assert server.sys_dir == sys_dir
        assert server.sys_name == sys_name
        assert server.partition_num == "1"

        assert type(server.profile) == DataFrame
        assert type(server.graph) == DataFrame
        assert type(server.edges) == DataFrame

    def test_Init2(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "2")
        assert server.sys_dir == sys_dir
        assert server.sys_name == sys_name
        assert server.partition_num == "2"

        assert type(server.profile) == DataFrame
        assert type(server.posts) == DataFrame
    
    def test_Init3(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "3")
        assert server.sys_dir == sys_dir
        assert server.sys_name == sys_name
        assert server.partition_num == "3"

        assert type(server.profile) == DataFrame
        assert type(server.comments) == DataFrame

    def test_AppendHop(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "1")
        follow = Follow(1, 1, "Adam", 2)
        comment = Comment(1, 1, "Adam", "Hello")
        post = Post(1, 1, "Adam", "Hello World")
        hops = [comment, post]
        server.appendHop(follow)

        assert len(server.hops) == 1
        assert server.hops[0] == follow

        server.appendHop(hops)
        assert len(server.hops) == 3
        assert server.hops[1] == comment
        assert server.hops[2] == post

    def test_RunHopsFollow(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "1")

        follow = Follow(1, 1, "adam", 2)
        server.appendHop(follow)
        server.runHops()

        assert len(server.edges) == 2
        assert server.edges.iloc[1]["edge_id"] == "1-2"

        server.appendHop(server.outgoing_hops)
        server.clearOutgoing()
        server.runHops()
        
        assert len(server.graph) == 2
        assert server.graph.iloc[1]["graph_id"] == 1
        assert server.graph.iloc[1]["user_id"] == 1
        assert server.graph.iloc[1]["follower_id"] == 2
        
        server.appendHop(server.outgoing_hops)
        server.clearOutgoing()
        server.runHops()

        assert len(server.profile) == 1
        assert server.profile.iloc[0]["user_id"] == 1
        assert server.profile.iloc[0]["username"] == "adam"
        assert server.profile.iloc[0]["followers"] == 26
        assert server.profile.iloc[0]["comments"] == 50
        assert server.profile.iloc[0]["posts"] == 3

        follow = Follow(1, 1, "adam", 2)
        server.appendHop(follow)
        server.runHops()

        assert len(server.edges) == 2
        assert len(server.outgoing_hops) == 0

    def test_RunHopsUnfollow(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "1")

        follow = Unfollow(2, 1, "adam", 3)
        server.appendHop(follow)
        server.runHops()

        assert len(server.edges) == 0

        server.appendHop(server.outgoing_hops)
        server.clearOutgoing()
        server.runHops()

        assert len(server.graph) == 0

        server.appendHop(server.outgoing_hops)
        server.clearOutgoing()
        server.runHops()

        assert len(server.profile) == 1
        assert server.profile.iloc[0]["user_id"] == 1
        assert server.profile.iloc[0]["username"] == "adam"
        assert server.profile.iloc[0]["followers"] == 24
        assert server.profile.iloc[0]["comments"] == 50
        assert server.profile.iloc[0]["posts"] == 3

        assert len(server.outgoing_hops) == 0

    def test_RunHopsPost(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "2")
        
        post = Post(1,1,"Joe","Hello World")
        server.appendHop(post)
        server.runHops()
        
        assert len(server.posts) == 1
        assert server.posts.iloc[0]["post_id"] == 1
        assert server.posts.iloc[0]["user_id"] == 1
        assert server.posts.iloc[0]["content"] == "Hello World"

        server.appendHop(server.outgoing_hops)
        server.clearOutgoing()
        server.runHops()

        assert len(server.profile) == 1
        assert server.profile.iloc[0]["user_id"] == 1
        assert server.profile.iloc[0]["username"] == "Joe"
        assert server.profile.iloc[0]["followers"] == 25
        assert server.profile.iloc[0]["comments"] == 50
        assert server.profile.iloc[0]["posts"] == 4

        assert len(server.outgoing_hops) == 0

    def test_RunHopsComment(self):
        root_dir = os.getcwd()
        sys_name = "test"
        sys_dir = os.path.join(os.path.join(root_dir, "systems"), sys_name)
        server = Server("test", sys_dir, "3")
        
        comment = Comment(1,1,"Tyler", "Nice Profile!")
        server.appendHop(comment)
        server.runHops()

        assert len(server.comments) == 1
        assert server.comments.iloc[0]["comment_id"] == 1
        assert server.comments.iloc[0]["user_id"] == 1
        assert server.comments.iloc[0]["comment"] == "Nice Profile!"

        server.appendHop(server.outgoing_hops)
        server.clearOutgoing()
        server.runHops()

        assert len(server.profile) == 1
        assert server.profile.iloc[0]["user_id"] == 1
        assert server.profile.iloc[0]["username"] == "Tyler"
        assert server.profile.iloc[0]["followers"] == 25
        assert server.profile.iloc[0]["comments"] == 51
        assert server.profile.iloc[0]["posts"] == 3

        assert len(server.outgoing_hops) == 0
        

        


