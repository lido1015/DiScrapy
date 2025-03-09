import os
from fastapi import APIRouter, UploadFile, File, Form, Request, status, HTTPException
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
import aiohttp
import asyncio
from aiohttp import FormData


from utils.utils import is_between, hash_key, update_storage, get_folder_name
from utils.const import API_PORT

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter()\


@router.post("/replicate")
async def replicate_data(
    request: Request,
    url: str = Form(...),  
    content: UploadFile = File(...)    
):
    node = request.app.state.node

    try:
        filename = f"{get_folder_name(url)}.zip"
        file_path = os.path.join(node.storage_dir, filename)
        
        content_bytes = await content.read()
        with open(file_path, "wb") as f:
            f.write(content_bytes)
        
        update_storage(node.storage_dir, url) 
        node.storage_set.add(url) 
        logger.info(f"Url {url} obtenida mediante replicacion")               
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Replicación exitosa"}
        )
    except Exception as e:
        logger.error(f"Error en replicación: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": f"Error en replicación: {str(e)}"}
        )


async def send_replication_request(node_ip: str, url_to_replicate: str, content: bytes):
    endpoint_url = f"http://{node_ip}:{API_PORT}/replicate"
    
    try:
        async with aiohttp.ClientSession() as session:
            data = FormData()
            data.add_field("url", url_to_replicate)  # Form field
            data.add_field(
                "content",  # Nombre del campo que coincide con el endpoint
                content,
                filename=f"{get_folder_name(url_to_replicate)}.zip",
                content_type="application/octet-stream"
            )
        
            async with session.post(endpoint_url, data=data) as response:
                if response.status != 200:
                    response_text = await response.text()
                    logger.error(f"Error replicando a {node_ip}: {response_text}")
                else:
                    logger.info(f"Replicación exitosa a {node_ip}")

    except Exception as e:
        logger.error(f"Error de conexión con {node_ip}: {str(e)}")


async def replicate_data_to_neighbors(node):
    neighbors = []        
    
    if node.succ and node.succ.ip != node.ip:
        neighbors.append(node.succ.ip)
    
    if node.pred and node.pred.ip != node.ip and node.pred.ip != node.succ.ip:
        neighbors.append(node.pred.ip)

    # Obtener URLs de responsabilidad del nodo
    my_urls = set()
    for url in node.storage_set:
        key = hash_key(url)
        if is_between(key, node.pred.id if node.pred else 0, node.id):
            my_urls.add(url)

    # Usar sesión única para mejor performance
    async with aiohttp.ClientSession() as session:
        for neighbor in neighbors:
            try:
                # Obtener URLs del vecino
                async with session.get(f"http://{neighbor}:{API_PORT}/urls") as response:
                    if response.status == 200:
                        neighbor_urls = set(await response.json())
                        urls_to_replicate = my_urls - neighbor_urls

                        # Replicar cada URL
                        for url in urls_to_replicate:
                            filename = f"{get_folder_name(url)}.zip"
                            file_path = os.path.join(node.storage_dir, filename)
                            
                            try:
                                with open(file_path, "rb") as f:
                                    content = f.read()
                                    await send_replication_request(neighbor, url, content)
                            except FileNotFoundError:
                                logger.error(f"Archivo no encontrado para replicación: {url}")
                            except Exception as e:
                                logger.error(f"Error leyendo archivo {url}: {str(e)}")

            except aiohttp.ClientConnectorError:
                logger.error(f"No se pudo conectar al vecino {neighbor}")
            except Exception as e:
                logger.error(f"Error general con vecino {neighbor}: {str(e)}")