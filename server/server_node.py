import sys
import signal
from chord.chord_node import ChordNode
from utils.utils import hash_key
import aiohttp
import requests
import asyncio

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
import uvicorn
import time
import socket
import shutil
from urllib.parse import quote


from multiprocessing import Process
import os



from scraper import scrape


API_HOST = "0.0.0.0"
API_PORT = 8000
M = 32


class ServerNode(ChordNode):
    def __init__(self):
        super().__init__(socket.gethostbyname(socket.gethostname()),)        
      
        # Crear carpeta de almacenamiento del nodo
        self.storage_dir = os.path.join('database', str(self.ip))
        os.makedirs(self.storage_dir,exist_ok=True)        
        with open(os.path.join(self.storage_dir, 'index.txt'), 'a'):
            pass

        #self.lock = asyncio.Lock()
        self.storage_set = set()

        self.app = FastAPI()      
        self.configure_endpoints() 
        self.api_process = None      

        signal.signal(signal.SIGINT, self._graceful_shutdown)
        signal.signal(signal.SIGTERM, self._graceful_shutdown)

        self.start()

    def start(self):
        super().start_server()        
        self._start_api_process()      
          
    def _start_api_process(self):                   
        self.api_process = Process(target = uvicorn.run(self.app,host=API_HOST,port=API_PORT))
        self.api_process.start()    

    def configure_endpoints(self):    
        @self.app.post("/scrape/")
        async def scrape_request(url: str):

            # Determine responsible node using Chord
            key = hash_key(url, M)
            responsible_node_ip = self.conn.find_succ(key)
            responsible_node_id = hash_key(responsible_node_ip, M)

            if responsible_node_id != self.id:
                logger.info(f"Redirecting scrape request of {url} to responsible node: {responsible_node_ip}")
                responsible_url = f"http://{responsible_node_ip}:{API_PORT}/scrape/?url={quote(url)}"
                return RedirectResponse(responsible_url)

              
            #if url not in self.storage_set:
            #async with self.lock:
            if url not in self.storage_set:
                logger.info("Scraping " + url)
                scrape(url, self.storage_dir)                    
                self.update_storage(url) 
                self.storage_set.add(url)

            return self._serve_file(url)
            

            

            # def _replicate_data(self, url):
    #     """Replica los datos a los nodos sucesores"""
    #     replicas = self.get_successors(REPLICATION_FACTOR)
    #     for node in replicas:
    #         if node.id != self.id:
    #             self._grpc_send_data(node, url, data)

    def _serve_file(self, url):
        zip_file = f"{self.folder_name(url)}.zip"
        path = os.path.join(self.storage_dir, zip_file)
        return FileResponse(
            path=path,
            filename=zip_file,
            media_type='application/zip'
        )

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


    def read_storage(self) -> list[str]:
        list = []
        index_file = os.path.join(self.storage_dir, 'index.txt')
        if os.path.exists(index_file):
            with open(index_file, 'r') as archivo:
                list = archivo.read().splitlines()
        return list

    def update_storage(self, url: str) -> None:
        index_file = os.path.join(self.storage_dir, 'index.txt')
        with open(index_file, 'a') as archivo:        
            archivo.write(url + '\n')   

    def folder_name(self, url):
        url = url[:-1]
        return url.split("//")[-1].replace("/", "_")


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







