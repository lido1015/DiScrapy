import os
import zipfile
import shutil
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError


from utils.utils import hash_key, update_storage, get_folder_name
from utils.const import API_PORT

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter()

oauth2 = OAuth2PasswordBearer(tokenUrl="scrape")
ALGORITHM = "HS256"
SECRET = "1652e68e6e5c4c9d21c6c38a87c143ea3f0b865fe318fae0374de808f5f0016f"

@router.post("/scrape")
async def scrape_request(url: str, request: Request, token: str = Depends(oauth2)): 

    try:
        payload = verify_jwt(token, SECRET) 
    except:
        raise HTTPException(
                status_code = 401,
                detail="Invalid credentials. Please log in.",
                headers = {"WWW-Authenticate":"Bearer"}
            )
        

    node = request.app.state.node

    try:

        # Determine responsible node using Chord
        key = hash_key(url)
        owner_node = node.find_succ(node.ref,key)        

        if owner_node.id != node.id:
            logger.info(f"Redirecting scrape request of {url} to responsible node: {owner_node.ip}")
            url = f"http://{owner_node.ip}:{API_PORT}/scrape?url={url}"
            return RedirectResponse(url)
        
        if url not in node.storage_set:
            async with node.lock:
                if url not in node.storage_set:
                    logger.info(f"Iniciando scraping de {url}")
                    try:
                        scrape(url, node.storage_dir)                            
                    except Exception as e:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Error durante el scraping: {str(e)}"
                        )    
                    update_storage(node.storage_dir,url) 
                    node.storage_set.add(url)   
                    logger.info(f"Scraping de {url} finalizado con exito")            

        return serve_file(node.storage_dir,url)
    
    except HTTPException:
        raise  # Re-lanza las excepciones HTTP ya manejadas
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"message": f"Error interno del servidor: {str(e)}"}
        )
        

def serve_file(storage_dir, url):
        zip_file = f"{get_folder_name(url)}.zip"
        path = os.path.join(storage_dir, zip_file)
        return FileResponse(
            path=path,
            filename=zip_file,
            media_type='application/zip'
        )


def scrape(url: str, path: str, only_html = True) -> None:

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error al acceder a la URL: {str(e)}") from e   

    folder = create_folder(url,path)
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    with open(os.path.join(folder, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(soup.prettify(formatter="html")) 

    if not only_html:
        download_files(url, soup, folder)

    compress(folder)

     


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




def verify_jwt(token: str, secret_key: str, algorithm: str = "HS256") -> dict:    
    try:
        # Decodificar y verificar automáticamente
        payload = jwt.decode(
            token,
            secret_key,
            algorithms=[algorithm],  # ¡Importante! Evita ataques de algoritmo
            options={"verify_exp": True, "require": ["exp"]},  # Verificar expiración
        )
        return payload
    
    except Exception as e:        
        raise Exception(f"Token inválido: {str(e)}")