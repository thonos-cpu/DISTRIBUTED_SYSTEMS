
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import time
from DHT import DHT
import pickle
import csv
import os


def make_chunks(n_rows: int, batch_size: int):
    chunks = []
    for start in range(0, n_rows, batch_size):
        end = min(start + batch_size, n_rows)
        chunks.append((start, end))
    return chunks

def process_chunk(start: int, end: int):
    df = pd.read_csv(CSV_PATH, skiprows=range(1, start + 1), nrows=end - start)
    out = []
    for _, row in df.iterrows():
        filtered_data = {
            "id": row.get("id"),
            "release_date": row.get("release_date"),
            "title": row.get("title")
        }
        out.append((row["title"], filtered_data))
    return out


CSV_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/output.csv"
LOOKUP_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/random_movie_names.csv"
replication_factor = 3



df_info = pd.read_csv(CSV_PATH, usecols=["title"])
n_rows = len(df_info)
batch_size = 50000
chunks = make_chunks(n_rows, batch_size)

def make_nodes(d : DHT, number: int) -> float:
    start_time = time.perf_counter()
    for i in range(number):
        d.join(f"node{i}")
    end_time = time.perf_counter()
    return 1000000*(end_time - start_time)
    
def insert_keys(d: DHT) -> float:
    start_time = time.perf_counter()
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as ex:
        futures = [ex.submit(process_chunk, s, e) for (s, e) in chunks]
        for fut in futures:
            pairs = fut.result()
            for title, attrs in pairs:
                d.put(title, attrs, replication_factor)
    end_time = time.perf_counter()
    return 1000000*(end_time - start_time)

def lookups(d : DHT) -> float:
    hops = 0
    with open(LOOKUP_PATH, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            key, owner, movie_list, hop = d.get(row[0])[0]
            hops = hops + hop
    return hops / 900

def run_simulation(x):
    d = DHT(m_bits=64)
    node_time = make_nodes(d, x)
    insert_time = insert_keys(d)
    lookup_time = lookups(d)
    return f"For {x} nodes: make_nodes={node_time}μs, insert={insert_time}μs, lookup={lookup_time} hops"
        
if __name__ == "__main__":
    for x in range(20, 300, 20):
        d = DHT(m_bits=64)
        print("For ", x, "nodes it took ", make_nodes(d, x),"μs to create the nodes ", insert_keys(d), "μs for the keys and " , lookups(d), "average hops for a lookup")