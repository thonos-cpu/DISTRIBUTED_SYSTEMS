import pandas as pd
from src.pastry.dht import PastryDHT

CSV_PATH = "C:/Users/stavr/Decentralised Data/data/data_movies_clean_utf8.csv"

print("Loading movie dataset...")
df = pd.read_csv(CSV_PATH)
title_column = 'title' if 'title' in df.columns else 'original_title'
movies = df[title_column].dropna().tolist()
print(f"Number of movies loaded: {len(movies)}")

dht = PastryDHT()
num_nodes = 10
for i in range(1, num_nodes + 1):
    dht.join(f"Node{i}")

print("Storing movies in DHT...")
for title in movies:
    dht.put(title, df[df[title_column] == title].iloc[0].to_dict())

print("DHT ready for lookup!")

while True:
    title = input("Δώσε τίτλο ταινίας (ή 'exit' για έξοδο): ")
    if title.lower() == 'exit':
        break
    node = dht.route_to_node(title)
    value = node.data.get(title)
    if value:
        print(f"Η ταινία '{title}' βρίσκεται στο node {node.id_str}, τιμές:")
        for k, v in value.items():
            print(f"  {k}: {v}")
    else:
        print(f"Η ταινία '{title}' δεν υπάρχει στο DHT")
