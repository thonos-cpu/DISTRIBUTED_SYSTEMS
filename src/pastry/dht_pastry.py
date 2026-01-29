from typing import Optional, Any, List
from concurrent.futures import ThreadPoolExecutor

from .node_pastry import Node
from ..common.hash_utils import hash_to_int
from .utils_pastry import normalize_title
import math

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
        new_node = Node(node_id, self.b)

        if not self.nodes:
            self.nodes.append(new_node)
            return new_node

        # Pastry join routing
        current = self.nodes[0]

        max_steps = int(math.log2(len(self.nodes) + 1)) + 5
        steps = 0

        while True:
            steps += 1
            next_node = current.route(new_node.id)

            current.update_routing_table(new_node)
            new_node.update_routing_table(current)

            if next_node.id == current.id or steps >= max_steps:
                break

            current = next_node

        # Leaf set
        self.nodes.append(new_node)

        for n in self.nodes:
            if n.id != new_node.id:
                new_node.update_leaf_set(n)
                n.update_leaf_set(new_node)
        return new_node


    def leave(self, node_id: int) -> None:
        node = next((n for n in self.nodes if n.id == node_id), None)
        if not node:
            return

        self.nodes.remove(node)

        for n in self.nodes:
            # leaf set cleanup
            n.leaf_set = [x for x in n.leaf_set if x.id != node_id]

            # routing table cleanup
            for i in range(len(n.routing_table)):
                for j in range(len(n.routing_table[i])):
                    if n.routing_table[i][j] and n.routing_table[i][j].id == node_id:
                        n.routing_table[i][j] = None


    # -----------------------------
    # DHT operations
    # -----------------------------
    def put(self, title: str, movie_id: int, value: Any) -> Node:
        """
        Insert movie using UNIQUE key (title, movie_id)
        """
        
        norm_title = normalize_title(title)
        if norm_title is None:
            return

        node, _ = self.locate_node(norm_title)
        node.data[norm_title].append(value)
        return node
    
    def update(self, title: str, movie_id: int, new_attrs: dict) -> bool:
        norm_title = normalize_title(title)
        if norm_title is None:
            return False

        node, _ = self.locate_node(norm_title)

        if norm_title not in node.data:
            return False

        for i, movie in enumerate(node.data[norm_title]):
            if movie.get("id") == movie_id:
                node.data[norm_title][i] = new_attrs
                return True

        return False


    def delete(self, title: str, movie_id: int) -> bool:
        norm_title = normalize_title(title)
        if norm_title is None:
            return False

        node, _ = self.locate_node(norm_title)

        if norm_title not in node.data:
            return False

        movies = node.data[norm_title]

        for i, movie in enumerate(movies):
            if movie.get("id") == movie_id:
                del movies[i]

                if not movies:
                    del node.data[norm_title]

                return True

        return False


    def get(self, title: str):
        if not self.nodes:
            return [], 0, None

        norm_title = normalize_title(title)
        if norm_title is None:
            return [], 0, None

        node, hops = self.locate_node(norm_title)
        # Επιστρέφουμε: (λίστα ταινιών, αριθμός hops, το ID του κόμβου)
        return node.data.get(norm_title, []), hops, node.id_str


    def get_parallel(self, title: str, k: int = 4):
        norm_title = normalize_title(title)
        if norm_title is None:
            return [], 0

        results = []
        hop_counts = []

        def lookup():
            node, hops = self.locate_node(norm_title)
            return node, hops

        with ThreadPoolExecutor(max_workers=k) as executor:
            futures = [executor.submit(lookup) for _ in range(k)]

            for f in futures:
                node, hops = f.result()
                if norm_title in node.data:
                    results.extend(node.data[norm_title])
                    hop_counts.append(hops)

        return results, min(hop_counts) if hop_counts else ([], 0)


    def locate_node(self, key):
        key_id = hash_to_int(key, self.m_bits)
        current = self.nodes[0]
        hops = 0
        max_hops = int(math.log2(len(self.nodes))) + 2

        while hops < max_hops:
            hops += 1
            next_node = current.route(key_id)
            if next_node.id == current.id:
                return current, hops
            current = next_node

        return current, hops
