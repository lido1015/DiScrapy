import os
import sys
import signal
import uvicorn
import time
import socket
import shutil
import aiohttp
import asyncio
from aiohttp import FormData
from multiprocessing import Process
from urllib.parse import quote
from fastapi import FastAPI, UploadFile, File, Form, status
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from chord.chord_node import ChordNode
from utils import hash_key, is_between, scrape, update_storage, get_folder_name

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


API_HOST = "0.0.0.0"
API_PORT = 8000
M = 32
REPLICATION_INTERVAL = 10

class ServerNode(ChordNode):
    def __init__(self):
        super().__init__(socket.gethostbyname(socket.gethostname()),)        
      
        # Crear carpeta de almacenamiento del nodo
        self.storage_dir = os.path.join('database', str(self.ip))
        os.makedirs(self.storage_dir,exist_ok=True)        
        with open(os.path.join(self.storage_dir, 'index.txt'), 'a'):
            pass

        self.lock = asyncio.Lock()
        self.storage_set = set()        

        self.app = FastAPI()   
        self.app.add_event_handler("startup", self.start_replication_loop)
        self.configure_endpoints() 
        self.api_process = None      

        signal.signal(signal.SIGINT, self._graceful_shutdown)
        signal.signal(signal.SIGTERM, self._graceful_shutdown)

        self.start()

    def start(self):
        super().start_server()        
        self._start_api_process()      
          
    def _start_api_process(self):                   
        self.api_process = Process(target = uvicorn.run(self.app,host=API_HOST,port=API_PORT,log_level="error"))
        self.api_process.start() 

    
    def start_replication_loop(self):
        asyncio.create_task(self.replication_loop())

    async def replication_loop(self):
        while True:
            await asyncio.sleep(REPLICATION_INTERVAL)
            try:
                await self.replicate_data_to_neighbors()
            except Exception as e:
                logger.error(f"Error en tarea de replicación: {str(e)}")   

    

    def configure_endpoints(self):        
        @self.app.get("/urls")
        async def get_urls():
            return JSONResponse(content=list(self.storage_set))

        @self.app.post("/scrape")
        async def scrape_request(url: str):

            # Determine responsible node using Chord
            key = hash_key(url, M)
            responsible_node_ip = self.conn.find_succ(key)
            responsible_node_id = hash_key(responsible_node_ip, M)

            if responsible_node_id != self.id:
                logger.info(f"Redirecting scrape request of {url} to responsible node: {responsible_node_ip}")
                responsible_url = f"http://{responsible_node_ip}:{API_PORT}/scrape?url={quote(url)}"
                return RedirectResponse(responsible_url)
              
            if url not in self.storage_set:
                async with self.lock:
                    if url not in self.storage_set:
                        logger.info("Scraping " + url)
                        scrape(url, self.storage_dir)                    
                        update_storage(self.storage_dir,url) 
                        self.storage_set.add(url)                

            return self._serve_file(url)

        @self.app.post("/replicate")
        async def replicate_data(
            url: str = Form(...),  # <-- Form field
            content: UploadFile = File(...)
        ):
            try:
                filename = f"{get_folder_name(url)}.zip"
                file_path = os.path.join(self.storage_dir, filename)
                
                content_bytes = await content.read()
                with open(file_path, "wb") as f:
                    f.write(content_bytes)
                
                update_storage(self.storage_dir, url) 
                self.storage_set.add(url) 
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


    async def send_replication_request(self, node_ip: str, url_to_replicate: str, content: bytes):
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

   
    async def replicate_data_to_neighbors(self):
        neighbors = []        
        
        if self.succ and self.succ.ip != self.ip:
            neighbors.append(self.succ.ip)
        
        if self.pred and self.pred.ip != self.ip and self.pred.ip != self.succ.ip:
            neighbors.append(self.pred.ip)

        # Obtener URLs de responsabilidad del nodo
        my_urls = set()
        for url in self.storage_set:
            key = hash_key(url, M)
            if is_between(key, self.pred.id if self.pred else 0, self.id):
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
                                file_path = os.path.join(self.storage_dir, filename)
                                
                                try:
                                    with open(file_path, "rb") as f:
                                        content = f.read()
                                        await self.send_replication_request(neighbor, url, content)
                                except FileNotFoundError:
                                    logger.error(f"Archivo no encontrado para replicación: {url}")
                                except Exception as e:
                                    logger.error(f"Error leyendo archivo {url}: {str(e)}")

                except aiohttp.ClientConnectorError:
                    logger.error(f"No se pudo conectar al vecino {neighbor}")
                except Exception as e:
                    logger.error(f"Error general con vecino {neighbor}: {str(e)}")

        
    def _serve_file(self, url):
        zip_file = f"{get_folder_name(url)}.zip"
        path = os.path.join(self.storage_dir, zip_file)
        return FileResponse(
            path=path,
            filename=zip_file,
            media_type='application/zip'
        )
            

            

            # def _replicate_data(self, url):
    #     """Replica los datos a los nodos sucesores"""
    #     replicas = self.get_successors(REPLICATION_FACTOR)
    #     for node in replicas:
    #         if node.id != self.id:
    #             self._grpc_send_data(node, url, data)

    

    def _graceful_shutdown(self, signum, frame):        
        logger.info("Iniciando apagado controlado...")        
        
        p = self.api_process
        if p and p.is_alive():
            logger.info(f"Deteniendo proceso {p.name}...")
            try:
                p.terminate()
                p.join(timeout=2)
                if p.exitcode is None:
                    logger.warning(f"Forzando terminación de {p.name}")
                    p.kill()
                    p.join()
            except Exception as e:
                logger.error(f"Error deteniendo {p.name}: {str(e)}")     
        
        # Eliminar almacenamiento
        shutil.rmtree(self.storage_dir, ignore_errors=True)
        
        # Detener servidor Chord
        self.stop_server()
        logger.info("Nodo detenido correctamente")
        sys.exit(0)


def main():
    node = ServerNode()
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("\nInterrupción recibida")
        node._graceful_shutdown(None, None)    

if __name__ == "__main__":
    main()







