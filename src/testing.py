import pandas as pd
import time
from tqdm import tqdm
from src.pastry.dht_pastry import PastryDHT


# Διαδρομές Αρχείων
DATA_CSV = r"C:/Users/stavr/Decentralised Data/data/data_movies_clean_utf8.csv"
test_csv = r"C:/Users/stavr/Decentralised Data/data/random_movie_names.csv"

def run_experiment():
    print("% Φόρτωση αρχείων στη μνήμη...")
    try:
        df_movies = pd.read_csv(DATA_CSV)
        all_movies = df_movies.to_dict('records')
        
        # Φόρτωση ΚΑΙ ΤΩΝ 29912 τίτλων
        df_bench = pd.read_csv(test_csv, header=None)
        lookup_titles = df_bench[0].dropna().tolist()
        
        total_bench_size = len(lookup_titles)
        print(f"% Ξεκινάει το πλήρες benchmark για {total_bench_size} τίτλους.")
    except Exception as e:
        print(f"Error: {e}")
        return

    node_counts = range(20, 301, 20)
    time_coords = []
    hop_coords = []

    for n_count in node_counts:
        # Δημιουργία δικτύου
        dht = PastryDHT(m_bits=64)
        for i in range(n_count):
            dht.join(f"Node{i}")
        
        # Εισαγωγή ταινιών
        for m in all_movies:
            dht.put(m["title"], m["id"], m)
            
        total_time = 0
        total_hops = 0
        
        # Αναζήτηση σε ΟΛΟΥΣ τους τίτλους
        for title in tqdm(lookup_titles, desc=f"Nodes {n_count}", leave=False):
            start_t = time.perf_counter()
            _, hops, _ = dht.get(str(title))
            end_t = time.perf_counter()
            total_time += (end_t - start_t)
            total_hops += hops
        
        avg_time = total_time / total_bench_size
        avg_hops = total_hops / total_bench_size
        
        time_coords.append(f"({n_count}, {avg_time:.8f})")
        hop_coords.append(f"({n_count}, {avg_hops:.2f})")

    print("\n" + "="*40)
    print("% ΤΕΛΙΚΑ ΑΠΟΤΕΛΕΣΜΑΤΑ (FULL DATASET)")
    print("="*40)
    print("\n% Average Lookup Time (29912 titles)")
    print("coordinates { " + " ".join(time_coords) + " };")
    print("\n% Average Hops (29912 titles)")
    print("coordinates { " + " ".join(hop_coords) + " };")

if __name__ == "__main__":
    run_experiment()