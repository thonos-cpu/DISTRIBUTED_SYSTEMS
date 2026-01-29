
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

    with open("C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl", "wb") as f:
            pickle.dump(d, f)

    size_bytes = os.path.getsize("C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl")
    size_kb = size_bytes / 1024
    size_mb = size_kb / 1024

    os.remove("C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl")

    return size_mb

def lookups(d : DHT) -> float:
    hops = 0
    start_time = time.perf_counter()
    with open(LOOKUP_PATH, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            key, owner, movie_list, hop = d.get(row[0])[0]
            hops = hops + hop
    end_time = time.perf_counter()
    return hops / 29912

def run_simulation(x):
    d = DHT(m_bits=64)
    node_time = make_nodes(d, x)
    insert_time = insert_keys(d)
    lookup_time = lookups(d)
    print( f"With {x} nodes and a replication factor of {replication_factor}: avg hops = {lookup_time}")
    del d
        
if __name__ == "__main__":
    
    for i in range(1, 7):
        replication_factor = i
        run_simulation(300)