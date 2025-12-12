from typing import Optional, List, Dict, Any


def in_range(k: int, a: int, b: int, inclusive_right: bool = True) -> bool:
    if a < b:
        return a < k <= b if inclusive_right else a < k < b
    elif a > b:
        return (a < k or k <= b) if inclusive_right else (a < k or k < b)
    else:
        return k != a if inclusive_right else False


class Node:
    def __init__(self, node_id: int, m_bits: int):
        self.id: int = node_id
        self.successor: "Node" = self
        self.predecessor: Optional["Node"] = None
        self.fingers: List["Node"] = [self] * m_bits
        self.data: Dict[str, Any] = {}

    def __repr__(self) -> str:
        return f"Node(id={self.id})"

    def find_successor(self, key_id: int) -> "Node":
        if self.successor is self:
            return self
        if in_range(key_id, self.id, self.successor.id, True):
            return self.successor
        n0 = self.closest_preceding_node(key_id)
        if n0 is self:
            return self.successor
        return n0.find_successor(key_id)

    def closest_preceding_node(self, key_id: int) -> "Node":
        for finger in reversed(self.fingers):
            if finger is not None and in_range(finger.id, self.id, key_id, False):
                return finger
        return self


