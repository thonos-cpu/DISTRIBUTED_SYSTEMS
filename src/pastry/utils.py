import unicodedata
import re

def normalize_title(title) -> str:
    if not isinstance(title, str):
        return None

    # Unicode normalization (NFKC)
    title = unicodedata.normalize("NFKC", title)

    # Ενοποίηση παυλών
    title = title.replace("–", "-").replace("—", "-")

    # Μόνο strip (αφαίρεση κενών στην αρχή και στο τέλος)
    # ΑΦΑΙΡΕΣΑΜΕ ΤΟ .lower() για να έχουμε Exact Case Match
    title = title.strip() 

    # αφαίρεση πολλαπλών κενών
    title = re.sub(r"\s+", " ", title)

    if title == "":
        return None

    return title


def common_prefix_len(a: str, b: str) -> int:
    """
    Returns length of common prefix between two hex strings
    """
    i = 0
    while i < min(len(a), len(b)) and a[i] == b[i]:
        i += 1
    return i


def hex_digit_at(s: str, idx: int) -> int:
    """
    Returns integer value of hex digit at position idx
    """
    return int(s[idx], 16)
