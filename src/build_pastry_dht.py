import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, freeze_support
from tqdm import tqdm

from src.pastry.dht import PastryDHT

CSV_PATH = r"C:/Users/stavr/Decentralised Data/data/data_movies_clean_utf8.csv"
NUM_NODES = 100
M_BITS = 64
BATCH_SIZE = 50000


def make_chunks(n_rows: int, batch_size: int):
    for start in range(0, n_rows, batch_size):
        end = min(start + batch_size, n_rows)
        yield start, end


def process_chunk(start: int, end: int, csv_path: str):
    df = pd.read_csv(csv_path, skiprows=range(1, start + 1), nrows=end - start)
    movies = []
    for _, row in df.iterrows():
        movies.append((row["title"], row["id"], row.to_dict()))
    return movies


def insert_movies(dht: PastryDHT, movies):
    for title, movie_id, attrs in movies:
        dht.put(title, movie_id, attrs)


if __name__ == "__main__":
    freeze_support()

    print("Loading dataset size...")
    n_rows = len(pd.read_csv(CSV_PATH))
    print(f"Total rows: {n_rows}")

    # Create DHT
    dht = PastryDHT(m_bits=M_BITS)
    for i in range(NUM_NODES):
        dht.join(f"Node{i}")

    print(f"{NUM_NODES} Pastry nodes created")

    chunks = list(make_chunks(n_rows, BATCH_SIZE))
    print(f"{len(chunks)} chunks")

    all_movies = []

    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        futures = [
            executor.submit(process_chunk, s, e, CSV_PATH)
            for s, e in chunks
        ]

        for f in tqdm(as_completed(futures), total=len(futures)):
            all_movies.extend(f.result())

    print("Inserting movies into Pastry DHT...")
    insert_movies(dht, all_movies)
    print("Insertion complete!")

    # Interactive lookup
    while True:
        title = input("\nΔώσε τίτλο ταινίας (!@ για έξοδο): ")
        if title() == "!@":
            break

        movies = dht.get(title)
        if not movies:
            print("Δεν βρέθηκε ταινία")
            continue

        print(f"Βρέθηκαν {len(movies)} ταινίες:")
        for m in movies:
            node = dht.route_to_node((m["title"], m["id"]))
            print(f"- {m['title']} ({m.get('release_date', 'N/A')}) → Node {node.id_str}")
