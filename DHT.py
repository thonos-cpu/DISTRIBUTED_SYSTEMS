import bisect
from node import Node
import xxhash
from typing import Dict, Optional, List, Any

class DHT:
    def __init__(self, m_bits: int):
        self.m_bits = m_bits
        self.M = 1 << m_bits
        self.mask = self.M - 1
        self.nodes: List[Node] = []

    def _hash_key(self, key: str) -> int:
        return xxhash.xxh64(str(key)).intdigest() & self.mask

    def _sorted_ids(self) -> List[int]:
        return [n.id for n in self.nodes]

    def _link_ring(self) -> None:
        n = len(self.nodes)
        for i, node in enumerate(self.nodes):
            node.successor = self.nodes[(i + 1) % n]
            node.predecessor = self.nodes[(i - 1) % n]

    def _rebuild_finger_tables(self) -> None:
        if not self.nodes: return
        ids = self._sorted_ids()
        n_nodes = len(self.nodes)
        
        for node in self.nodes:
            for i in range(self.m_bits):
                start = (node.id + (1 << i)) & self.mask
                pos = bisect.bisect_left(ids, start)
                node.fingers[i] = self.nodes[pos % n_nodes]

    def find_successor(self, key_id: int):
        if not self.nodes: return None, 0
        return self.nodes[0].find_successor(key_id)

    def join(self, node_name: str) -> Node:
        hashed = self._hash_key(node_name)
        new_node = Node(hashed, self.m_bits)
        bisect.insort(self.nodes, new_node, key=lambda x: x.id)
        self._link_ring()
        self._rebuild_finger_tables()
        return new_node
    
    def leave(self, node: Node):
        
        if node is None:
            raise ValueError(f"Node is not part of this DHT")

        if len(self.nodes) == 1:
            node.data.clear()
            self.nodes.clear()
            return

        self.nodes.remove(node)
        self._link_ring()
        self._rebuild_finger_tables()

    def put(self, key: str, value: Any, r: int) -> Node:
        h = self._hash_key(key)
        owner, _ = self.find_successor(h)
        if key not in owner.data:
            owner.data[key] = []
            owner.data[key].append(value)
            for x in range(r):
                owner = owner.successor
                owner.data[key] = []
                owner.data[key].append(value)
        return owner

    def get(self, key: str) -> List[Any]:
        h = self._hash_key(key)
        owner, hops = self.find_successor(h)
        results = owner.data.get(key, [])
        return [(key, owner, results, hops)]