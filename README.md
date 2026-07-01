# Chord-style Distributed Hash Table

An in-memory simulation of a [Chord](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)
distributed hash table, written as a coursework project for the Distributed
Systems course at the University of Patras. It was built to **measure** how
lookup cost and data availability behave as the ring grows and as nodes fail.

> **Scope, honestly:** this is a single-process *simulation*, not a networked
> service. There are no sockets or RPC — all nodes live in one Python process.
> The goal is to study the routing algorithm and replication scheme, not to
> ship a production key-value store.

## What it does

- **Consistent hashing** — keys and nodes are placed on a `2**m_bits` ring with
  a 64-bit xxhash.
- **Finger tables** — each node keeps `m_bits` fingers, giving `O(log N)`
  lookups instead of a linear walk around the ring.
- **Replication** — each key is copied toward the opposite side of the ring for
  resilience, so a lookup can fall back to a replica if the primary is gone.
- **Churn** — nodes can `join` and `leave` at any time; finger tables and
  successor/predecessor links are rebuilt on each change.

## API

```python
from DHT import DHT

dht = DHT(m_bits=16)            # ring over a 2^16 identifier space

dht.join("node-a")             # add a node (named); returns the Node
dht.put("key", value, r=3)     # store with r replicas; returns the primary owner
dht.get("key")                 # -> [(key, owner, [values], hops)]
dht.leave(node)                # remove / fail a node
```

| Method | Description |
| --- | --- |
| `DHT(m_bits)` | Create an empty ring over `2**m_bits` ids. |
| `join(name) -> Node` | Hash `name` onto the ring and insert the node. |
| `put(key, value, r) -> Node` | Store `value` under `key` with `r` replicas; returns the primary owner. |
| `get(key) -> list` | Returns `[(key, owner, values, hops)]`; falls back to a replica if the primary is missing. |
| `leave(node)` | Remove a node (also used to simulate failure). |

## Files

```
DHT.py          # ring, consistent hashing, finger tables, put/get/replication
node.py         # Node class + Chord routing (find_successor, fingers)
build_DHT.py    # builds the ring from a movie CSV and runs failure experiments
testing.py      # sweeps the replication factor and reports average lookup hops
Technical_Report_Greek.pdf   # course report (Greek)
```

## The experiment

`build_DHT.py` and `testing.py` build the ring from a ~946K-row movie dataset
(key = title) using a `ProcessPoolExecutor` for parallel ingestion, then sweep
the replication factor and the fraction of failed nodes, recording the
**average lookup hop count** across tens of thousands of queries.

> These scripts contain hard-coded local paths to the dataset; edit the paths
> at the top of each file to run them against your own CSV.

## References

- Stoica et al., *Chord: A Scalable Peer-to-peer Lookup Service* (SIGCOMM 2001).
