from .node import Node
from typing import Optional, Any
from ..common.hash_utils import hash_to_int

class PastryDHT:
    def __init__(self, m_bits: int = 64, b: int = 4):
        self.m_bits = m_bits
        self.nodes: list[Node] = []
        self.b = b

    def join(self, node_name: str) -> Node:
        node_id = hash_to_int(node_name, self.m_bits)
        new_node = Node(node_id, self.b)
        self.nodes.append(new_node)
        return new_node

    def leave(self, node_name: str) -> None:
        node_id = hash_to_int(node_name, self.m_bits)
        node_to_remove = next((n for n in self.nodes if n.id == node_id), None)
        if node_to_remove:
            for k, v in node_to_remove.data.items():
                self.put(k, v)
            self.nodes.remove(node_to_remove)

    def put(self, key: str, value: Any) -> Node:
        if not self.nodes:
            raise RuntimeError("No nodes in the network")
        node = self.route_to_node(key)
        node.data[key] = value
        return node

    def get(self, key: str) -> Optional[Any]:
        for n in self.nodes:
            if key in n.data:
                return n.data[key]
        return None

    def route_to_node(self, key: str) -> Node:
        if not self.nodes:
            raise RuntimeError("No nodes in the network")
        key_id = hash_to_int(key, self.m_bits)
        current = self.nodes[0]
        hops = 0
        while True:
            hops += 1
            leaf_ids = [n.id for n in current.leaf_set] + [current.id]
            if key_id in leaf_ids:
                return current
            prefix_len = 0
            key_hex = hex(key_id)[2:]
            for a, b in zip(current.id_str, key_hex):
                if a == b:
                    prefix_len += 1
                else:
                    break
            if prefix_len < len(current.id_str):
                col = int(key_hex[prefix_len], 16)
                next_node = current.routing_table[prefix_len][col]
                if next_node:
                    current = next_node
                    continue
            current = min(self.nodes, key=lambda n: abs(n.id - key_id))
            return current
