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
import threading
import time

from multiprocessing import Process


import os
import argparse

from scraper import scrape



class ServerNode(ChordNode):
    def __init__(self, port, contact_port=None):
        
        super().__init__(port, contact_port)
        self.storage = read_storage()
        self.app = FastAPI()
        self.fastapi_port = int(port) + 1  
        self.configure_endpoints()
        self.fastapi_process = None

        signal.signal(signal.SIGINT, self._graceful_shutdown)
        signal.signal(signal.SIGTERM, self._graceful_shutdown)

    def _graceful_shutdown(self, signum, frame):
        logger.info("Señal de terminación recibida")
        self.stop()

    def start(self):
        """Inicia gRPC en el hilo principal y FastAPI en proceso separado"""
        # Iniciar gRPC usando la lógica existente de ChordNode
        super().start_server()  # <-- Esto ya maneja hilos internos
        
        # Iniciar FastAPI en un proceso
        self._start_fastapi_process()

    def _start_fastapi_process(self):
        """Crea y arranca el proceso de FastAPI"""
        
        self.fastapi_process = Process(target = uvicorn.run(self.app,host="localhost",port=self.fastapi_port))
        self.fastapi_process.start()

  

    def stop(self):
        """Detiene ambos servicios limpiamente"""
        super().stop_server()  # Detiene gRPC
        if self.fastapi_process:
            self.fastapi_process.terminate()  # Detiene FastAPI
            self.fastapi_process.join(timeout=2)
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



# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--ip", type=str, default="127.0.0.1")
#     parser.add_argument("--port", type=str, default=10000)    
#     parser.add_argument("--contact_port", type=str, default=None)

#     try:
#         args = parser.parse_args()     
#         node = ServerNode(args.port, args.contact_port)
#         while True:
#             time.sleep(3600)
#     except SystemExit as e:
#         print(f"Error: {e}, argumentos recibidos: {sys.argv}")   
#     except KeyboardInterrupt:
#         logger.info("\nInterrupción recibida")
#     except Exception as e:
#         logger.error(f"Error crítico: {str(e)}")
#     finally:
#         node.stop_server()  


def main():
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    


    if len(sys.argv) < 2:
        print("Uso: python chord_node.py <puerto> [puerto_contacto]")
        sys.exit(1)

    port = sys.argv[1]
    contact_port = sys.argv[2] if len(sys.argv) > 2 else None    

    node = ServerNode(port,contact_port)

    try:
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







