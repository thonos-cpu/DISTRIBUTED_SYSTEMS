from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import time
from DHT import DHT 
import pickle
import csv
import os

CSV_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/output.csv"
PICKLE_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl"

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
        out.append((row["title"], row.to_dict()))
    return out

if __name__ == "__main__":
    d = None
    if os.path.exists(PICKLE_PATH):
        print("Loading existing DHT from pickle...")
        with open(PICKLE_PATH, "rb") as f:
            d = pickle.load(f)
    else:
        print("No .pkl file found. Building new DHT...")
        df_info = pd.read_csv(CSV_PATH, usecols=["title"])
        n_rows = len(df_info)
        batch_size = 50000 

        chunks = make_chunks(n_rows, batch_size)
        d = DHT(m_bits=64)
        
        for i in range(300):
            d.join(f"node{i}")

        start_time = time.perf_counter()
        
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as ex:
            futures = [ex.submit(process_chunk, s, e) for (s, e) in chunks]
            for fut in futures:
                pairs = fut.result()
                for title, attrs in pairs:
                    d.put(title, attrs)

        with open(PICKLE_PATH, "wb") as f:
            pickle.dump(d, f)
            
        print(f"Build completed in {time.perf_counter() - start_time:.2f} seconds.")


    def lookup():
        while True:
            title = input("\nΔώστε όνομα ταινίας (ή !@ για έξοδο): ")
            if title == "!@":
                break

            start = time.perf_counter()
            results = d.get(title)
            end = time.perf_counter()

            if not results or not results[0][2]:
                print("No Movies found.")
            else:
                key, owner, movie_list, hops = results[0]
                print(f"\nFound {len(movie_list)} results in {owner} ({hops} hops):")
                print("-" * 50)
                
                for i, movie in enumerate(movie_list, 1):
                    print(f"RESULTS {i}:")
                    for field, value in movie.items():
                        print(f"  {field}: {value}")
                    print("-" * 30)
            
            print(f"Lookup Time: {end - start:.6f} sec")
    
    def find_3_random_movies():
        results = []
        for node in d.nodes:
            count = 0
            for title, movie_info in node.data.items():
                if isinstance(movie_info, list):
                    for movie in movie_info:
                        if count < 3:
                            results.append([movie.get('title')]) 
                            count += 1
                        else: break
                else:
                    if count < 3:
                        results.append([movie_info.get('title')])
                        count += 1
                
                if count >= 3:
                    break

        print(results)
        with open('C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/random_movie_names.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(results)

    #find_3_random_movies()
    
    #lookup()

    #Melodies of War