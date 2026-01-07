from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import time
from DHT import DHT 
import pickle
from pprint import pprint


CSV_PATH = r"C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/output.csv"


def make_chunks(n_rows: int, batch_size: int):
    chunks = []
    for start in range(0, n_rows, batch_size):
        end = min(start + batch_size, n_rows)
        chunks.append((start, end))
    return chunks


def process_chunk(start: int, end: int):
    df = pd.read_csv(CSV_PATH)

    df_chunk = df.iloc[start:end]

    out = []
    for _, row in df_chunk.iterrows():
        title = row["title"]
        attrs = row.to_dict()
        out.append((title, attrs))
    return out


if __name__ == "__main__":

    try:
        with open("C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl", "rb") as f:
            d = pickle.load(f)
    except Exception as e:
        df = pd.read_csv(CSV_PATH)
        n_rows = len(df)
        print("No .pkl file found")
        batch_size = 100000

        chunks = make_chunks(n_rows, batch_size)
        print(f"Total rows: {n_rows}, batch_size: {batch_size}, chunks: {len(chunks)}")

        d = DHT(m_bits=64)
        for i in range(100):
            d.join("node" + str(i))

        start = time.perf_counter()

        
        max_workers = min(len(chunks), 12)
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(process_chunk, s, e) for (s, e) in chunks]

            for fut in futures:
                pairs = fut.result()
                for title, attrs in pairs:
                    d.put(title, attrs)

        with open("C:/Users/tasis/Desktop/sxoli/DISTRIBUTED_SYSTEMS/my_dht.pkl", "wb") as f:
            pickle.dump(d, f)

        end = time.perf_counter()
        print("Execution time:", end - start, "seconds")
        print(f"Loaded {n_rows} rows with batch_size={batch_size} and {len(chunks)} chunks.")


#Node 18174345339414816917: 2204 items
#Node 18408725440168399977: 10041 items


    start = time.perf_counter()

    for i, (k, v) in enumerate(d.nodes[80].data.items()):
        if i == 5:
            break
        print(k, v)


    print(d.get("The Life of Charles Peace"))


    end = time.perf_counter()
    print("Execution time:", end - start, "seconds")
    


