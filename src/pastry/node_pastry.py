from typing import Dict, List, Any, Optional
from collections import defaultdict
from .utils_pastry import common_prefix_len, hex_digit_at




class Node:
    def __init__(self, node_id: int, b: int = 4, leaf_size: int = 4):
        self.id: int = node_id
        self.id_str: str = hex(node_id)[2:].zfill(16)
        self.leaf_min: Optional[int] = None
        self.leaf_max: Optional[int] = None


        # title -> list of movies
        self.data: Dict[str, List[Any]] = defaultdict(list)

        self.leaf_set: List["Node"] = []
        self.b = b
        self.base = 2 ** b
        self.leaf_size = leaf_size

        self.routing_table: List[List[Optional["Node"]]] = [
            [None for _ in range(self.base)]
            for _ in range(len(self.id_str))
        ]

    def update_leaf_set(self, other: "Node"):
        if other.id == self.id:
            return

        self.leaf_set.append(other)

        # κρατάμε μοναδικούς
        self.leaf_set = list({n.id: n for n in self.leaf_set}.values())

        # sort by ID distance
        self.leaf_set.sort(key=lambda n: abs(n.id - self.id))
        self.leaf_set = self.leaf_set[: self.leaf_size]

        # cache min / max
        ids = [n.id for n in self.leaf_set]
        self.leaf_min = min(ids)
        self.leaf_max = max(ids)



    def update_routing_table(self, other: "Node"):
        l = common_prefix_len(self.id_str, other.id_str)
        if l >= len(self.routing_table):
            return

        digit = hex_digit_at(other.id_str, l)
        if self.routing_table[l][digit] is None:
            self.routing_table[l][digit] = other


    def route(self, key_id: int) -> "Node":
        key_str = hex(key_id)[2:].zfill(len(self.id_str))

        # 1. Έλεγχος στο Leaf Set
        if self.leaf_set:
            if self.leaf_min <= key_id <= self.leaf_max:
                closest_leaf = min(self.leaf_set + [self], key=lambda n: abs(n.id - key_id))
                return closest_leaf

        # 2. Prefix Routing (Routing Table)
        l = common_prefix_len(self.id_str, key_str)
        
        # --- ΠΡΟΣΘΗΚΗ ΑΥΤΟΥ ΤΟΥ ΕΛΕΓΧΟΥ ---
        if l == len(self.id_str):
            return self
        # ----------------------------------

        digit = hex_digit_at(key_str, l)
        next_node = self.routing_table[l][digit]
        
        if next_node is not None:
            return next_node
        # 3. Rare case / Fallback: Greedy routing
        # Αν αποτύχουν τα παραπάνω, βρες οποιονδήποτε γνωστό κόμβο που μειώνει την απόσταση
        all_known_nodes = [n for row in self.routing_table for n in row if n is not None]
        all_known_nodes.extend(self.leaf_set)
        
        best_node = self
        min_dist = abs(self.id - key_id)
        
        for n in all_known_nodes:
            dist = abs(n.id - key_id)
            if dist < min_dist:
                min_dist = dist
                best_node = n
        
        return best_node




if __name__ == "__main__":
    n = Node(12345)
    print(n.id_str)
    n.data["Batman"].append({"id": 1})
    n.data["Batman"].append({"id": 2})
    print(n.data)
