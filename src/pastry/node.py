from typing import Dict, List, Any, Optional
from collections import defaultdict


class Node:
    def __init__(self, node_id: int, b: int = 4, leaf_size: int = 4):
        self.id: int = node_id
        self.id_str: str = hex(node_id)[2:]

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


if __name__ == "__main__":
    n = Node(12345)
    print(n.id_str)
    n.data["Batman"].append({"id": 1})
    n.data["Batman"].append({"id": 2})
    print(n.data)
