import os
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
from server_node import ServerNode  

from utils.utils import hash_key, scrape, update_storage, get_folder_name
from utils.const import API_PORT

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/scrape")
async def scrape_request(url: str, request: Request):

    node: ServerNode = request.app.state.node

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