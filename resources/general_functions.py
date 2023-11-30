# Function to calculate SHA256 hash for the concatenation of the first three columns
import hashlib

def calculate_hash(concatenated_string):
    sha256_hash = hashlib.sha256(concatenated_string.encode()).hexdigest()
    return sha256_hash