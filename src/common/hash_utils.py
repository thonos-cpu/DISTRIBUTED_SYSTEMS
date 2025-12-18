import xxhash

def hash_to_int(key: str, m_bits: int = 64) -> int:
    """Μετατρέπει ένα string σε ακέραιο μέσω xxhash"""
    key = str(key)
    mask = (1 << m_bits) - 1
    return xxhash.xxh64(key).intdigest() & mask

if __name__ == "__main__":
    # Παράδειγμα
    print(hash_to_int("Avatar"))
    print(hash_to_int("The Matrix"))
    print(hash_to_int("Random Movie Title"))
