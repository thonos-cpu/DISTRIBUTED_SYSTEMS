import pandas as pd
import time
import random
import string
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, freeze_support
from .pastry.utils_pastry import normalize_title
from tqdm import tqdm




from src.pastry.dht_pastry import PastryDHT

CSV_PATH = r"C:/Users/stavr/Decentralised Data/data/data_movies_clean_utf8.csv"
BENCH_CSV_PATH = r"C:/Users/stavr/Decentralised Data/data/random_movie_names.csv"
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

def load_titles_from_csv(csv_path: str):
    with open(csv_path, "r", encoding="utf-8") as f:
        titles = [line.strip() for line in f if line.strip()]
    return titles


if __name__ == "__main__":
    freeze_support()

    # ===============================
    # BUILD DHT TIMER START
    # ===============================
    build_start = time.perf_counter()

    print("------Loading dataset size------")
    n_rows = len(pd.read_csv(CSV_PATH))
    print(f"Total rows: {n_rows}")


    def insert_movies(dht: PastryDHT, movies):
        for i, (title, movie_id, attrs) in enumerate(movies):
            if i % 100000 == 0:
                print(f"Inserted {i} movies...")
            dht.put(title, movie_id, attrs)


    # --- Create DHT nodes ---
    t_nodes_start = time.perf_counter()

    dht = PastryDHT(m_bits=M_BITS)
    for i in range(NUM_NODES):
        dht.join(f"Node{i}")

    t_nodes_end = time.perf_counter()
    print(f"[TIME] Node creation: {t_nodes_end - t_nodes_start:.6f} sec")

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

    print("\n------Running lookup benchmark------")

    
    csv_titles = load_titles_from_csv(BENCH_CSV_PATH)
    lookup_titles = csv_titles[:NUM_LOOKUPS]


    lookup_times = []
    hop_counts = []


    for title in lookup_titles:
        start = time.perf_counter()
        _, hops, _ = dht.get(normalize_title(title)) 
        end = time.perf_counter()
        lookup_times.append(end - start)
        hop_counts.append(hops)


    avg_lookup = sum(lookup_times) / len(lookup_times)
    print(f"Avg hops per lookup: {sum(hop_counts)/len(hop_counts):.2f}")


    print(f"[LOOKUP] Average lookup time over {NUM_LOOKUPS} queries: "
      f"{avg_lookup:.6f} sec")



    # ===============================
    # PARALLEL LOOKUP BENCHMARK
    # ===============================
    print("\n------Running PARALLEL lookup benchmark------")

    parallel_times = []

    for title in lookup_titles:
        start = time.perf_counter()
        dht.get_parallel(title)
        end = time.perf_counter()
        parallel_times.append(end - start)

    avg_parallel = sum(parallel_times) / len(parallel_times)

    print(f"[PARALLEL LOOKUP] Average time over {NUM_LOOKUPS} queries: "
        f"{avg_parallel:.6f} sec")




    # ===============================
    # INSERT BENCHMARK
    # ===============================
    NUM_INSERTS = 1000

    print("\n------Running insert benchmark------")

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

    print("\n------[UPDATE DEMO]------")

    movies, _, _ = dht.get(demo_title)
    target = [m for m in movies if m["id"] == demo_id][0]

    print("======Before update:")
    print(target)

    updated_attrs = target.copy()
    updated_attrs["popularity"] += 10
    updated_attrs["vote_average"] = min(updated_attrs["vote_average"] + 1, 10)
    updated_attrs["updated"] = True

    dht.update(demo_title, demo_id, updated_attrs)

    movies_after, _, _ = dht.get(demo_title)
    target_after = [m for m in movies_after if m["id"] == demo_id][0]

    print("======After update:")
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


    # ===============================
    # DELETE DEMO (before / after)
    # ===============================
    demo_title, demo_id, _ = all_movies[1]

    print("\n------[DELETE DEMO]------")

    movies_before, _, _ = dht.get(demo_title)
    print(f"------Before delete ({len(movies_before)} movies):")
    for m in movies_before:
        print(f"  - {m['title']} ({m['id']})")

    dht.delete(demo_title, demo_id)

    movies_after, _, _ = dht.get(demo_title)
    print(f"\n------After delete ({len(movies_after)} movies):")
    for m in movies_after:
        print(f"  - {m['title']} ({m['id']})")




    # ===============================
    # DELETE BENCHMARK
    # ===============================
    NUM_DELETES = 100

    print("\n------Running delete benchmark------")

    delete_times = []
    sample_movies = random.sample(all_movies, NUM_DELETES)

    for title, movie_id, _ in sample_movies:
        start = time.perf_counter()
        dht.delete(title, movie_id)
        end = time.perf_counter()
        delete_times.append(end - start)

    avg_delete = sum(delete_times) / len(delete_times)

    print(f"[DELETE] Average delete time over {NUM_DELETES} deletes: "
        f"{avg_delete:.6f} sec")
    


    # ===============================
    # NODE JOIN DEMO
    # ===============================
    print("\n------[NODE JOIN DEMO]-------")

    initial_nodes = len(dht.nodes)
    print(f"Nodes before join: {initial_nodes}")

    t_join_start = time.perf_counter()
    dht.join("NewNode_1")
    t_join_end = time.perf_counter()

    print(f"Nodes after join: {len(dht.nodes)}")
    print(f"[JOIN TIME] {t_join_end - t_join_start:.6f} sec")


    # ===============================
    # NODE LEAVE DEMO
    # ===============================
    print("\n------[NODE LEAVE DEMO]------")

    # κρατάμε reference στο node
    node_to_leave = dht.nodes[0]
    leave_id = node_to_leave.id

    print("Node ID that will leave:", node_to_leave.id_str)

    print("\nSample node IDs BEFORE leave:")
    print([n.id_str for n in dht.nodes[:5]])

    nodes_before = len(dht.nodes)

    t_leave_start = time.perf_counter()
    dht.leave(leave_id)
    t_leave_end = time.perf_counter()

    print("Sample node IDs AFTER leave:")
    print([n.id_str for n in dht.nodes[:5]])

    nodes_after = len(dht.nodes)

    print(f"\nNodes before leave: {nodes_before}")
    print(f"Nodes after leave: {nodes_after}")
    print(f"[LEAVE TIME] {t_leave_end - t_leave_start:.6f} sec")

    # απόλυτη απόδειξη
    exists = any(n.id == leave_id for n in dht.nodes)
    print("Does left node still exist?", exists)



    # ===============================
    # NODE JOIN / LEAVE BENCHMARK
    # ===============================
    NUM_NODE_EVENTS = 10
    join_times = []
    leave_times = []

    for i in range(NUM_NODE_EVENTS):
        name = f"TempNode_{i}"

        t1 = time.perf_counter()
        dht.join(name)
        t2 = time.perf_counter()
        join_times.append(t2 - t1)

        t3 = time.perf_counter()
        dht.leave(name)
        t4 = time.perf_counter()
        leave_times.append(t4 - t3)

    print(f"[JOIN] Avg time: {sum(join_times)/len(join_times):.6f} sec")
    print(f"[LEAVE] Avg time: {sum(leave_times)/len(leave_times):.6f} sec")

     # Επιλογή ενός τυχαίου κόμβου για να δούμε το Leaf Set του
    sample_node = dht.nodes[0] 
    print(f"Leaf Set of Node {sample_node.id_str}: {[n.id_str for n in sample_node.leaf_set]}")



    # --- Interactive lookup ----------------------------------------------------------------------
    while True:
        title = input("\nΔώστε τίτλο ταινίας (!@ για έξοδο): ")
        if title == "!@":
            break

        t_lookup_start = time.perf_counter()
        # Στέλνουμε το title ΩΣ ΕΧΕΙ, η dht.get θα το κάνει normalize
        movies, hops, node_id = dht.get(title) 
        t_lookup_end = time.perf_counter()

        if not movies:
            print(f"Δεν βρέθηκε ταινία (Normalized search for: '{normalize_title(title)}')")
            continue

        print(f"Βρέθηκαν {len(movies)} ταινίες | hops: {hops} | time: {t_lookup_end - t_lookup_start:.6f}s")
        for m in movies:
            print(f"- {m['title']} ({m.get('release_date', 'N/A')}) --> Node {node_id}")
