import bisect
from node import Node
import xxhash
from typing import Optional, List


def in_range(k: int, a: int, b: int, inclusive_right: bool = True) -> bool:
    if a < b:
        return a < k <= b if inclusive_right else a < k < b
    elif a > b:
        return (a < k or k <= b) if inclusive_right else (a < k or k < b)
    else:
        return k != a if inclusive_right else False

class DHT:
    def __init__(self, m_bits: int):
        self.m_bits = m_bits
        self.M = 1 << m_bits
        self.mask = self.M - 1
        self.nodes: List[Node] = []

    def _hash_node_id(self, name: str) -> int:
        name = str(name)
        return xxhash.xxh64(name).intdigest() & self.mask

    def _hash_key(self, key: str) -> int:
        key = str(key)
        return xxhash.xxh64(key).intdigest() & self.mask

    def _sorted_ids(self) -> List[int]:
        return [n.id for n in self.nodes]

    def _link_ring(self) -> None:
        if not self.nodes:
            return
        n = len(self.nodes)
        nodes = self.nodes
        for i, node in enumerate(nodes):
            node.successor = nodes[(i + 1) % n]
            node.predecessor = nodes[(i - 1) % n]

    def _rebuild_finger_tables(self, ids: Optional[List[int]] = None) -> None:
        if not self.nodes:
            return
        if ids is None:
            ids = self._sorted_ids()

        nodes = self.nodes
        n_nodes = len(nodes)
        m_bits = self.m_bits
        M = self.M
        mask = M - 1

        last_ids = getattr(self, "_last_ids", None)
        curr_set = set(ids)
        prev_set = set(last_ids) if last_ids is not None else None

        def succ_by_start(start: int) -> Node:
            pos = bisect.bisect_left(ids, start)
            return nodes[0] if pos == n_nodes else nodes[pos]

        def node_by_id(x: int) -> Node:
            pos = bisect.bisect_left(ids, x)
            if pos == n_nodes or ids[pos] != x:
                raise RuntimeError("Inconsistent ring state")
            return nodes[pos]

        def pred_of_id(x: int) -> Node:
            pos = bisect.bisect_left(ids, x)
            if pos == 0:
                return nodes[-1]
            return nodes[pos - 1]

        def full_rebuild() -> None:
            for node in nodes:
                base = node.id
                fingers = node.fingers
                for i in range(m_bits):
                    start = (base + (1 << i)) & mask
                    fingers[i] = succ_by_start(start)

        if last_ids is None:
            full_rebuild()
            self._last_ids = ids[:]
            return

        inserted = list(curr_set - prev_set)
        removed = list(prev_set - curr_set)

        if len(inserted) + len(removed) != 1:
            full_rebuild()
            self._last_ids = ids[:]
            return

        if len(inserted) == 1:
            new_id = inserted[0]
            new_node = node_by_id(new_id)

            base = new_node.id
            fingers = new_node.fingers
            for i in range(m_bits):
                start = (base + (1 << i)) & mask
                fingers[i] = succ_by_start(start)

            for i in range(m_bits):
                x = (new_id - (1 << i)) & mask
                p = pred_of_id(x)
                start_p = p
                while True:
                    f = p.fingers[i]
                    if f is None or in_range(new_id, p.id, f.id, True):
                        if f is None or f.id != new_id:
                            p.fingers[i] = new_node
                            p = p.predecessor
                            if p is None or p.id == start_p.id:
                                break
                            continue
                    break

            self._last_ids = ids[:]
            return

        removed_id = removed[0]
        removed_key = str(removed_id)

        for node in nodes:
            base = node.id
            fingers = node.fingers
            for i in range(m_bits):
                f = fingers[i]
                if f is None:
                    continue
                if f.id == removed_id or str(f.id) == removed_key:
                    start = (base + (1 << i)) & mask
                    fingers[i] = succ_by_start(start)

        self._last_ids = ids[:]


    def _any_node(self) -> Optional[Node]:
        return self.nodes[0] if self.nodes else None

    def find_successor(self, key_id: int):
        if not self.nodes:
            raise RuntimeError("DHT is empty; cannot route.")
        owner, hops = self.nodes[0].find_successor(key_id)
        return owner, hops

    def join(self, node_name: str) -> Node:
        hashed = self._hash_node_id(node_name)
        for n in self.nodes:
            if n.id == hashed:
                raise ValueError(f"Node id {hashed} already exists in ring")
        new_node = Node(hashed, self.m_bits)
        ids = self._sorted_ids()
        pos = bisect.bisect_left(ids, hashed)
        self.nodes.insert(pos, new_node)
        self._link_ring()
        ids = self._sorted_ids()
        self._rebuild_finger_tables(ids)

        if len(self.nodes) == 1:
            return new_node

        pred = new_node.predecessor
        succ = new_node.successor
        d_succ = succ.data
        d_new = new_node.data
        keys_to_move = []

        for k in d_succ.keys():
            h = self._hash_key(k)
            if in_range(h, pred.id, new_node.id, True):
                keys_to_move.append(k)

        for k in keys_to_move:
            d_new[k] = d_succ.pop(k)

        return new_node

    def leave(self, node_name: str) -> None:
        hashed = self._hash_node_id(node_name)

        node = None
        for n in self.nodes:
            if n.id == hashed:
                node = n
                break

        if node is None:
            raise ValueError(f"Node '{node_name}' is not part of this DHT")

        if len(self.nodes) == 1:
            node.data.clear()
            self.nodes.clear()
            return

        self.nodes.remove(node)
        self._link_ring()
        ids = self._sorted_ids()
        self._rebuild_finger_tables(ids)

    def put(self, key: str, value: any) -> Node:
        if not self.nodes:
            raise RuntimeError("Cannot put key into an empty DHT")
        h = self._hash_key(key)
        owner, _ = self.find_successor(h)
        owner.data[key] = value
        return owner

    def get(self, key: str, default: any = None) -> any:
        if not self.nodes:
            return default
        h = self._hash_key(key)
        owner, hops = self.find_successor(h)
        return owner.data.get(key, default), hops

    def delete(self, key: str) -> bool:
        if not self.nodes:
            return False
        h = self._hash_key(key)
        owner, _ = self.find_successor(h)
        if key in owner.data:
            del owner.data[key]
            return True
        return False
