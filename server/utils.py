import os
import hashlib
import zipfile
import shutil
import requests
from bs4 import BeautifulSoup
import urljoin
from urllib.parse import urljoin


    #============CHORD============

def hash_key(key: str, m: int) -> int:
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
    url = url[:-1]
    return url.split("//")[-1].replace("/", "_")



    #============SCRAPING============


def scrape(url: str, path: str, only_html = True) -> None:

    response = requests.get(url)
    
    if response.status_code == 200:

        folder = create_folder(url,path)
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        with open(os.path.join(folder, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(soup.prettify(formatter="html")) 

        if not only_html:
            download_files(url, soup, folder)

        compress(folder)

    else:
        raise Exception(f"Error accessing {url}: {response.status_code}")    


def create_folder(url: str, path: str) -> str:

    folder_name = get_folder_name(url)
    
    folder_path = os.path.join(path, folder_name)

    try:
        os.mkdir(folder_path)        
        return folder_path       
    except FileExistsError:
        print(f"The folder '{folder_path}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating the folder: {e}")


def download_files(url, soup, folder):
    for css in soup.find_all("link", rel="stylesheet"):
        css_url = urljoin(url, css.get('href', '')) 
        if css_url:
            download(css_url, folder)

    for script in soup.find_all("script"):
        if script.attrs.get("src"):
            js_url = urljoin(url, script.attrs.get("src"))
            download(js_url, folder)



def download(url, folder):
    try:        
        response_file = requests.get(url)
        name_file = os.path.join(folder, os.path.basename(url))
        with open(name_file, 'wb') as f:
            f.write(response_file.content)   
        
    except Exception as e:
        print(f"Error downloading {url}: {e}")   



def compress(folder):

    zip_name = f"{folder}.zip"

    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)                
                zipf.write(file_path, os.path.relpath(file_path, folder))

    shutil.rmtree(folder)  