from common.hash_utils import hash_to_int

movies = [
    "Eiko",
    "Postcards",
    "Die Entführung aus dem Paradies",
    "Chumbo",
    "White Blood",
    "Onkel Vanja",
    "Ravan Raaj: A True Story",
    "The Annihilators",
    "Thou Shalt Not",
    "The Phantom",
    "Post No Bills",
    "Venmegam",
    "Abnormal Criminal",
    "The Indian's Gift",
    "Real Beauty",
    "Jim",
    "Viajante",
    "Unthinkable",
    "A Woman Bought in an Auction",
    "Tomorrow It Will Be Better"
]

for title in movies:
    h = hash_to_int(title)
    print(f"{title} -> {h}")
