import sys
import signal
from chord.chord_node import ChordNode

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn
import time
import socket


from multiprocessing import Process, Queue, Manager
import os


from scraper import scrape


API_HOST = "0.0.0.0"
API_PORT = 8000


class ServerNode(ChordNode):
    def __init__(self):

        super().__init__(socket.gethostbyname(socket.gethostname()),)
        
        # Configuración de procesos y recursos compartidos
        self.manager = Manager()
        self.task_queue = Queue()
        self.shared_storage = self.manager.list()
        self.active = self.manager.Value('b', True)

        self.storage = read_storage()
        self.app = FastAPI()
        
        
        self.configure_endpoints()
        self.fastapi_process = None

        signal.signal(signal.SIGINT, self._graceful_shutdown)
        signal.signal(signal.SIGTERM, self._graceful_shutdown)
        

    def _graceful_shutdown(self, signum, frame):
        logger.info("Señal de terminación recibida")
        self.stop()
        sys.exit(0)

    def start(self):
        """Inicia gRPC en el hilo principal y FastAPI en proceso separado"""
        # Iniciar gRPC usando la lógica existente de ChordNode
        super().start_server()  # <-- Esto ya maneja hilos internos
        
        # Iniciar FastAPI en un proceso
        self._start_fastapi_process()

    def _start_fastapi_process(self):
        """Crea y arranca el proceso de FastAPI"""
        
        self.fastapi_process = Process(target = uvicorn.run(self.app,host=API_HOST,port=API_PORT))
        self.fastapi_process.start()

  

    def stop(self):
        """Detiene ambos servicios limpiamente"""
        self.stop_server()  # Detiene gRPC
        if self.fastapi_process and self.fastapi_process.is_alive():
            self.fastapi_process.terminate()  # Detiene FastAPI
            self.fastapi_process.join()
            self.fastapi_process = None
            logger.info("Servidor FastAPI detenido")
        
   

    def configure_endpoints(self):        

        @self.app.post("/scrape")        
        def scrape_request(url: str):
                    
            try:    
                if url not in self.storage:
                    print("Scraping " + url)  
                    scrape(url)                    
                    update_storage(url) 
                    self.storage.append(url)
                else:
                    print(f"The url {url} is already scraped.")

                zip_file = folder_name(url) + ".zip"
                path = os.path.join('storage/', zip_file)
                response = FileResponse(path=path,filename=zip_file,media_type='application/zip')             
                return response
            except Exception as e:                    
                return {"error": str(e)}  

      


def read_storage() -> list[str]:
    list = []
    with open('storage/index.txt', 'r') as archivo:
        list = archivo.read().splitlines()
    return list

def update_storage(url: str) -> None:
    with open('storage/index.txt', 'a') as archivo:        
        archivo.write(url + '\n')   

def folder_name(url):
    url = url[:-1]
    return url.split("//")[-1].replace("/", "_")




def main():    
    
  
    try:          
        node = ServerNode()
        
        node.start()
        # Bucle no bloqueante
        while node._running:  # Acceso directo al flag por simplicidad
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("\nInterrupción recibida")
    finally:
        node.stop()

if __name__ == "__main__":
    main()







