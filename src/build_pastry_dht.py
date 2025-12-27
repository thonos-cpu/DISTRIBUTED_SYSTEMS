import pandas as pd
import time
import random
import string
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

    # ===============================
    # BUILD DHT TIMER START
    # ===============================
    build_start = time.perf_counter()

    print("Loading dataset size...")
    n_rows = len(pd.read_csv(CSV_PATH))
    print(f"Total rows: {n_rows}")

    # --- Create DHT nodes ---
    t_nodes_start = time.perf_counter()

    dht = PastryDHT(m_bits=M_BITS)
    for i in range(NUM_NODES):
        dht.join(f"Node{i}")

    t_nodes_end = time.perf_counter()
    print(f"[TIME] Node creation: {t_nodes_end - t_nodes_start:.2f} sec")

    # --- Dataset chunking ---
    chunks = list(make_chunks(n_rows, BATCH_SIZE))
    print(f"{len(chunks)} chunks")

    # --- Parallel loading ---
    t_load_start = time.perf_counter()

    all_movies = []
    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        futures = [
            executor.submit(process_chunk, s, e, CSV_PATH)
            for s, e in chunks
        ]

        for f in tqdm(as_completed(futures), total=len(futures), desc="Loading CSV"):
            all_movies.extend(f.result())

    t_load_end = time.perf_counter()
    print(f"[TIME] Dataset loading: {t_load_end - t_load_start:.2f} sec")

    # --- Insert into DHT ---
    t_insert_start = time.perf_counter()

    insert_movies(dht, all_movies)

    t_insert_end = time.perf_counter()
    print(f"[TIME] Insert movies into DHT: {t_insert_end - t_insert_start:.2f} sec")




    # ===============================
    # BUILD DHT TIMER END
    # ===============================
    build_end = time.perf_counter()
    print(f"\n[TOTAL BUILD TIME] {build_end - build_start:.2f} sec")


    # ===============================
    # LOOKUP BENCHMARK
    # ===============================
    NUM_LOOKUPS = 100

    print("\nRunning lookup benchmark...")

    # παίρνουμε τυχαίους τίτλους
    random_titles = random.sample(all_movies, NUM_LOOKUPS)

    lookup_times = []

    for title, movie_id, _ in random_titles:
        start = time.perf_counter()
        dht.get(title)
        end = time.perf_counter()
        lookup_times.append(end - start)

    avg_lookup = sum(lookup_times) / len(lookup_times)

    print(f"[LOOKUP] Average lookup time over {NUM_LOOKUPS} queries: "
      f"{avg_lookup:.6f} sec")



    # ===============================
    # INSERT BENCHMARK
    # ===============================
    NUM_INSERTS = 1000

    print("\nRunning insert benchmark...")

    insert_times = []

    for i in range(NUM_INSERTS):
        fake_title = f"benchmark_movie_{i}"
        fake_attrs = {
            "title": fake_title,
            "id": f"bm_{i}",
            "year": 2025
        }

        start = time.perf_counter()
        dht.put(fake_title, f"bm_{i}", fake_attrs)
        end = time.perf_counter()

        insert_times.append(end - start)

    avg_insert = sum(insert_times) / len(insert_times)

    print(f"[INSERT] Average insert time over {NUM_INSERTS} inserts: "
      f"{avg_insert:.6f} sec")


    # ===============================
    # UPDATE DEMO (before / after)
    # ===============================
    demo_title, demo_id, demo_attrs = all_movies[0]

    print("\n[UPDATE DEMO]")

    movies = dht.get(demo_title)
    target = [m for m in movies if m["id"] == demo_id][0]

    print("Before update:")
    print(target)

    updated_attrs = target.copy()
    updated_attrs["popularity"] += 10
    updated_attrs["vote_average"] = min(updated_attrs["vote_average"] + 1, 10)
    updated_attrs["updated"] = True

    dht.update(demo_title, demo_id, updated_attrs)

    movies_after = dht.get(demo_title)
    target_after = [m for m in movies_after if m["id"] == demo_id][0]

    print("After update:")
    print(target_after)




    # ===============================
    # UPDATE BENCHMARK
    # ===============================
    NUM_UPDATES = 100

    print("\nRunning update benchmark...")

    update_times = []

    sample_movies = random.sample(all_movies, NUM_UPDATES)

    for title, movie_id, attrs in sample_movies:
        new_attrs = attrs.copy()
        new_attrs["updated"] = True

        start = time.perf_counter()
        dht.update(title, movie_id, new_attrs)
        end = time.perf_counter()

        update_times.append(end - start)


    avg_update = sum(update_times) / len(update_times)

    print(f"[UPDATE] Average update time over {NUM_UPDATES} updates: "
      f"{avg_update:.6f} sec")




    # --- Interactive lookup ---
    while True:
        title = input("\nΔώσε τίτλο ταινίας (!@ για έξοδο): ")
        if title == "!@":
            break

        t_lookup_start = time.perf_counter()
        movies = dht.get(title)
        t_lookup_end = time.perf_counter()

        if not movies:
            print("Δεν βρέθηκε ταινία")
            continue

        print(f"Βρέθηκαν {len(movies)} ταινίες "
              f"(lookup time: {t_lookup_end - t_lookup_start:.6f} sec):")

        for m in movies:
            node = dht.route_to_node((m["title"], m["id"]))
            print(f"- {m['title']} ({m.get('release_date', 'N/A')}) → Node {node.id_str}")

