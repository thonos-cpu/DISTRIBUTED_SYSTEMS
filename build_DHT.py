from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import time
from DHT import DHT
import pickle
import csv
import os
import random


CSV_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/output.csv"
PICKLE_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl"
replication_factor = 3

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
                    d.put(title, attrs, replication_factor)

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
            
            print(f"Lookup Time: {1000*(end - start):.6f} ms")
    
    def find_random_movies(max: int):
        results = []

        for node in d.nodes:
            pool = []

            for _, movie_info in node.data.items():
                if isinstance(movie_info, list):
                    pool.extend(movie_info)
                else:
                    pool.append(movie_info)

            if not pool:
                continue

            k = min(max, len(pool))
            picked = random.sample(pool, k)

            for movie in picked:
                results.append([movie.get("title")])

        with open(
            "C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/random_movie_names.csv",
            "w",
            newline="",
            encoding="utf-8"
        ) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(results)


    def node_keys_len():
        results = []
        for node in d.nodes:
            results.append([len(node.data)])
        with open('C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/movies_len_in_each_node_with_r_3.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(results)


    def save_nodes():
        results = []
        for node in d.nodes:
            results.append([node.id])
        with open('C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/nodes.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(results)

    def deletion(number: int, step: int):
        with open('C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/nodes.csv', newline='', encoding='utf-8') as f:
            temp_number = number
            rows = list(csv.reader(f))
            while temp_number>0:
                for i in range(step):
                    r = random.randrange(len(rows))
                    d.leave(next((n for n in d.nodes if n.id == int(rows[r][0])), None))
                    del rows[r]
                    with open('C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/random_movie_names.csv', newline='', encoding='utf-8') as c:
                        result_count = 0
                        hops = 0
                        reader_c = csv.reader(c)
                        for row in reader_c:
                            key, owner, movie_list, hop = d.get(row[0])[0]
                            hops = hops + hop
                temp_number = temp_number - step
                print("For", f"{(number - temp_number)/3:.2f}", "%",  " failed nodes and a replication factor of ", replication_factor, "we had avergae hops of:", f"{hops/29912:.2f}")




    #save_nodes() # save all the node hashes in acsv file for further testing
    #node_keys_len() # save the no. of movies each node hosts
    #find_random_movies(100) # function to save first 3 movies of each node in a csv file.

    #d.leave(next((n for n in d.nodes if n.id == 3125458981636820055), None))
    #d.leave(next((n for n in d.nodes if n.id == 13443093695648291354), None))
    #d.leave(next((n for n in d.nodes if n.id == 13463368274894646953), None))
    #d.leave(next((n for n in d.nodes if n.id == 13471430620621363024), None))



    #lookup()


    deletion(150, 10)