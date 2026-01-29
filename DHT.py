import bisect
from node import Node
import xxhash
from typing import Dict, Optional, List, Any

class DHT:
    def __init__(self, m_bits: int):
        self.m_bits = m_bits #number of bits the hash algorithm supports
        self.M = 1 << m_bits #left shift of m
        self.mask = self.M - 1
        self.nodes: List[Node] = [] #list of all the nodes the DHT has

    def _hash_key(self, key: str) -> int:
        return xxhash.xxh64(str(key)).intdigest() & self.mask #hashing algorithm, converted to an int

    def _sorted_ids(self) -> List[int]:
        return [n.id for n in self.nodes] # node sorting based on it's id

    def _link_ring(self) -> None:
        n = len(self.nodes)
        for i, node in enumerate(self.nodes):
            node.successor = self.nodes[(i + 1) % n] #set each node's successor
            node.predecessor = self.nodes[(i - 1) % n] #set each node's predecessor

    def _rebuild_finger_tables(self) -> None:
        if not self.nodes: return
        ids = self._sorted_ids()
        n_nodes = len(self.nodes)
        
        #rebuilding all finger tables for all nodes

        for node in self.nodes:
            for i in range(self.m_bits):
                start = (node.id + (1 << i)) & self.mask
                pos = bisect.bisect_left(ids, start)
                node.fingers[i] = self.nodes[pos % n_nodes]

    def find_successor(self, key_id: int):
        if not self.nodes: return None, 0
        return self.nodes[0].find_successor(key_id)

    def join(self, node_name: str) -> Node:
        hashed = self._hash_key(node_name) #hashing the node name
        new_node = Node(hashed, self.m_bits) # creating the node instance
        bisect.insort(self.nodes, new_node, key=lambda x: x.id) #insert the node in the correct position in the nodes list
        self._link_ring() #set the new node's successor & predecessor plus it's neighboor's
        self._rebuild_finger_tables() # rebuild the finger tables for the node and all the nodes before him
        return new_node
    
    def leave(self, node: Node):
        if node is None:
            raise ValueError(f"Node is not part of this DHT") 

        if len(self.nodes) == 1:
            node.data.clear()
            self.nodes.clear()
            return

        self.nodes.remove(node) #sudden node failure simulation
        self._link_ring()
        self._rebuild_finger_tables()

    def put(self, key: str, value: Any, r: int) -> Node:
        h = self._hash_key(key) #hash the movie tile, title = key
        owner, _ = self.find_successor(h) #find where the key should be hosted
        if key not in owner.data:
            owner.data[key] = [] 
        owner.data[key].append(value) #insert the key and the values to the correct node
        half_ring = 1 << (self.m_bits - 1) #find the opposite position of that node
        opposite_id = (owner.id + half_ring) & self.mask 
        current, _ = self.find_successor(opposite_id) 
        for _ in range(r):
            current.data[key] = list(owner.data[key]) #insert the data (as backup) to the opposite node and his 'r' successors 
            current = current.successor
        return owner

    def get(self, key: str) -> List[Any]:
        h = self._hash_key(key) #hash the query
        owner, hops = self.find_successor(h) #find to which node the query should be if exists
        results = owner.data.get(key, []) #check the node for the key = query
        if len(results) == 0: #if it doesnt exists it maybe due to a node failure so check for backups
            half_ring = 1 << (self.m_bits - 1)
            opposite_id = (h + half_ring) & self.mask
            owner, extra_hops = self.find_successor(opposite_id)
            hops += extra_hops
            #checks if the query exists to any of the opposite node or its 'r' successors
            results = owner.data.get(key, [])
        return [(key, owner, results, hops)]