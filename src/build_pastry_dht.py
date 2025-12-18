# src/build_pastry_dht.py
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, freeze_support
from tqdm import tqdm
from src.pastry.dht import PastryDHT
from src.common.hash_utils import hash_to_int  # Υποθέτουμε ότι υπάρχει

CSV_PATH = r"C:/Users/stavr/Decentralised Data/data/data_movies_clean_utf8.csv"
NUM_NODES = 10
M_BITS = 64
BATCH_SIZE = 50000


def make_chunks(n_rows: int, batch_size: int):
    chunks = []
    for start in range(0, n_rows, batch_size):
        end = min(start + batch_size, n_rows)
        chunks.append((start, end))
    return chunks


def process_chunk(start: int, end: int, csv_path: str):
    df = pd.read_csv(csv_path, skiprows=range(1, start+1), nrows=end-start)
    out = []
    for _, row in df.iterrows():
        title = row['title']
        out.append((title, row.to_dict()))
    return out


def insert_movies(dht, movie_pairs):
    for title, attrs in movie_pairs:
        dht.put(title, attrs)


if __name__ == "__main__":
    freeze_support()  # απαραίτητο για Windows multiprocessing

    print("Loading dataset length...")
    df = pd.read_csv(CSV_PATH)
    n_rows = len(df)
    print(f"Total rows: {n_rows}")

    # --- Δημιουργία Pastry DHT ---
    dht = PastryDHT(m_bits=M_BITS)
    for i in range(NUM_NODES):
        dht.join(f"Node{i}")
    print(f"{NUM_NODES} nodes joined")

    # --- Χωρίζουμε σε chunks ---
    chunks = make_chunks(n_rows, BATCH_SIZE)
    print(f"{len(chunks)} chunks, batch size: {BATCH_SIZE}")

    # --- Παραλληλία επεξεργασίας chunks ---
    max_workers = cpu_count()
    print(f"Using {max_workers} processes")

    all_pairs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, s, e, CSV_PATH) for s, e in chunks]
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing chunks"):
            pairs = fut.result()
            all_pairs.extend(pairs)

    print("Inserting movies into DHT...")
    insert_movies(dht, all_pairs)
    print("All movies inserted!")

    # --- Test lookup ---
    while True:
        title = input("Δώσε τίτλο ταινίας (ή 'exit' για έξοδο): ")
        if title.lower() == 'exit':
            break
        movie_data = dht.get(title)
        if movie_data:
            key_id = hash_to_int(title, M_BITS)
            node = dht.route_to_node(title)
            print(f"Η ταινία '{title}' βρίσκεται στο node {hex(node.id)[2:]}")
            for k, v in movie_data.items():
                print(f"  {k}: {v}")
        else:
            print(f"Η ταινία '{title}' δεν υπάρχει στο DHT")
