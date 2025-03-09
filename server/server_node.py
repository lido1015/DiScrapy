import os
import sys
import signal
import uvicorn
import time
import socket
import shutil
import asyncio
from multiprocessing import Process
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from chord.chord_node import ChordNode


import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from utils.const import HOST, API_PORT, REPLICATION_INTERVAL
from roles import scraper, replicator, authenticator



class ServerNode(ChordNode):
    def __init__(self):
        super().__init__(socket.gethostbyname(socket.gethostname()),)        
      
        # Crear carpeta de almacenamiento del nodo
        self.storage_dir = os.path.join('database', str(self.ip))
        os.makedirs(self.storage_dir,exist_ok=True)        
        with open(os.path.join(self.storage_dir, 'index.txt'), 'a'):
            pass        

        self.lock = asyncio.Lock()
        self.storage_set: set[str] = set()
        self.users_dict: dict[str,str] = {}       

        self.app = FastAPI()   
        self.app.state.node = self    
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
        self.api_process = Process(target = uvicorn.run(self.app,host=HOST,port=API_PORT,log_level="error"))
        self.api_process.start()  
   
    def start_replication_loop(self):
        asyncio.create_task(self.replication_loop())

    async def replication_loop(self):
        while True:            
            try:
                await replicator.replicate_data_to_neighbors(self)
            except Exception as e:
                logger.error(f"Error en tarea de replicación: {str(e)}")
            await asyncio.sleep(REPLICATION_INTERVAL)
    

    def configure_endpoints(self): 
        self.app.include_router(scraper.router)
        self.app.include_router(replicator.router)
        self.app.include_router(authenticator.router)

        @self.app.get("/urls")
        async def get_urls():
            return JSONResponse(content=list(self.storage_set))
        
        @self.app.get("/users")
        async def get_users():
            return JSONResponse(content=list(self.users_dict.items()))


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







