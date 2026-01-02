from typing import Optional, Any, List
from concurrent.futures import ThreadPoolExecutor

from .node import Node
from ..common.hash_utils import hash_to_int

class PastryDHT:
    def __init__(self, m_bits: int = 64, b: int = 4):
        self.m_bits = m_bits
        self.b = b
        self.nodes: List[Node] = []

    # -----------------------------
    # Node management
    # -----------------------------
    def join(self, node_name: str) -> Node:
        node_id = hash_to_int(node_name, self.m_bits)
        node = Node(node_id, self.b)
        self.nodes.append(node)
        return node

    def leave(self, node_id: int) -> None:
        """
        Simulated node leave.
        Data migration is omitted for performance reasons.
        """
        node = next((n for n in self.nodes if n.id == node_id), None)
        if not node:
            return

        self.nodes.remove(node)



    # -----------------------------
    # DHT operations
    # -----------------------------
    def put(self, title: str, movie_id: int, value: Any) -> Node:
        """
        Insert movie using UNIQUE key (title, movie_id)
        """
        key = (title, movie_id)
        node = self.route_to_node(key)
        node.data[title].append(value)
        return node
    
    def update(self, title: str, movie_id: int, new_attrs: dict) -> bool:
        """
        Update a specific movie (by title + id).
        """
        key = (title, movie_id)
        node = self.route_to_node(key)

        if title not in node.data:
            return False

        for i, movie in enumerate(node.data[title]):
            if movie.get("id") == movie_id:
                node.data[title][i] = new_attrs
                return True

        return False


    def delete(self, title: str, movie_id: int) -> bool:
        """
        Delete a specific movie (by title + id) from the DHT.
        """
        key = (title, movie_id)
        node = self.route_to_node(key)

        if title not in node.data:
            return False

        movies = node.data[title]

        for i, movie in enumerate(movies):
            if movie.get("id") == movie_id:
                del movies[i]

                # αν δεν έμειναν άλλες ταινίες με αυτό το title
                if not movies:
                    del node.data[title]

                return True

        return False



    def get(self, title: str):
        """
        Exact-match lookup by title.
        Returns ALL movies with this title.
        """
        results = []
        for node in self.nodes:
            if title in node.data:
                results.extend(node.data[title])
        return results
    
    def get_parallel(self, title: str):
        """
        Parallel exact-match lookup by title.
        Queries all nodes concurrently.
        """
        results = []

        def search_node(node):
            if title in node.data:
                return node.data[title]
            return []

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(search_node, node) for node in self.nodes]

            for f in futures:
                results.extend(f.result())

        return results



    # -----------------------------
    # Routing (simulation-level)
    # -----------------------------
    def route_to_node(self, key) -> Node:
        """
        Routes based on hashed key.
        Returns destination node.
        """
        key_id = hash_to_int(key, self.m_bits)
        return min(self.nodes, key=lambda n: abs(n.id - key_id))
