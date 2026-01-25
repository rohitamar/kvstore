from collections import defaultdict
from sortedcontainers import SortedDict
import mmh3 

class HashRing:
    def __init__(self, v_count: int = 100):
        self.v_count = v_count
        self.ring = SortedDict() 

    def get_index(self, key: str) -> int:
        hsh = mmh3.hash(key, signed=False)
        
        idx = self.ring.bisect_left(hsh)
        if idx == len(self.ring):
           idx = 0

        key = self.ring.keys()[idx]
        return self.ring[key]

    def add_node(self, node: str) -> None:
        for i in range(self.v_count):
            v_node_id = f"{node}-{i}"
            hsh = mmh3.hash(v_node_id, signed=False)    
            self.ring[hsh] = node

    def del_node(self, node: str) -> None:
        for i in range(self.v_count):
            v_node_id = f"{node}-{i}"
            hsh = mmh3.hash(v_node_id, signed=False)
            del self.ring[hsh]
    
    # ensures that the hash universe we generate with virtual nodes 
    # is roughly equal to each other
    # indicating that the hash space is equally partitioned
    def coverage(self):
        keys = list(self.ring.keys())
        node_coverage = defaultdict(int)

        for i in range(len(keys)):
            prev = keys[i - 1] if i > 0 else keys[-1]
            cur = keys[i]

            if cur > prev:
                distance = cur - prev 
            else:
                distance = (2 ** 32 - prev) + cur
            
            node_coverage[self.ring[cur]] += distance

        return node_coverage