import hashlib


# Function to hash a string using SHA-1 and return its integer representation
def hash_key(key: str, m: int) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest()[:16], 16) % (2**m)

# Function to check if n id is between two other id's in chord ring
def is_between(k: int, start: int, end: int) -> bool:
    if start < end:
        return start < k <= end
    else:  # The interval wraps around 0
        return start < k or k <= end