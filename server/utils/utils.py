import os
import hashlib


from utils.const import M


    #============CHORD============

def hash_key(key: str, m: int = M) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest()[:16], 16) % (2**m)

def is_between(k: int, start: int, end: int) -> bool:
    if start < end:
        return start < k <= end
    else:  # The interval wraps around 0
        return start < k or k <= end    


    #============MANAGE STORAGE============

def read_storage(storage_dir) -> set:
    list = []
    index_file = os.path.join(storage_dir, 'index.txt')
    if os.path.exists(index_file):
        with open(index_file, 'r') as archivo:
            list = archivo.read().splitlines()
    return set(list)

def update_storage(storage_dir, url: str) -> None:
    index_file = os.path.join(storage_dir, 'index.txt')
    with open(index_file, 'a') as archivo:        
        archivo.write(url + '\n')   

def get_folder_name(url):    
    return url.split("//")[-1].replace("/", "_")




