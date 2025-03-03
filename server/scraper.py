import os
import zipfile
import shutil

import requests
from bs4 import BeautifulSoup

import urljoin
from urllib.parse import urljoin




def scrape(url: str, path: str, only_http = True) -> None:

    response = requests.get(url)
    
    if response.status_code == 200:

        folder = create_folder(url,path)
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        with open(os.path.join(folder, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(soup.prettify(formatter="html")) 

        if not only_http:
            download_files(url, soup, folder)

        compress(folder)

    else:
        raise Exception(f"Error accessing {url}: {response.status_code}")    


def create_folder(url: str, path: str) -> str:

    folder_name = url[:-1].split("//")[-1].replace("/", "_")
    
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