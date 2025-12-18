from typing import Dict, Any, List, Optional

class Node:
    def __init__(self, node_id: int, b: int = 4, leaf_size: int = 4):
        self.id: int = node_id
        self.id_str: str = hex(node_id)[2:]
        self.data: Dict[str, Any] = {}
        self.leaf_set: List["Node"] = []
        self.b = b
        self.base = 2 ** b
        self.leaf_size = leaf_size
        self.routing_table: List[List[Optional["Node"]]] = [
            [None for _ in range(self.base)] for _ in range(len(self.id_str))
        ]

if __name__ == "__main__":
    test_node = Node(node_id=12345)
    print("Node ID:", test_node.id)
    print("Node hex ID:", test_node.id_str)
    print("Leaf set (αρχικά):", test_node.leaf_set)
    print("Routing table rows:", len(test_node.routing_table))
    print("Routing table cols:", len(test_node.routing_table[0]) if test_node.routing_table else 0)
