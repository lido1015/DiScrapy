from concurrent import futures
import hashlib
import sys
import threading
import time

import grpc
from chord.chord_client import Chord_gRPC_Client
from chord.protos.chord_pb2 import NodeMessage, IdMessage, StatusResponseMessage, EmptyMessage
import chord.protos.chord_pb2_grpc as pb

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def hash_key(key: str) -> int:
    return int(hashlib.sha1(key.encode()).hexdigest()[:16], 16) % (2**64)    

class ChordNode(pb.ChordServiceServicer):
    def __init__(self, port, contact_port):
        self.port = port
        self.id = int(port)
        self.ip = "localhost"
        self.contact_port = contact_port

        self.grpc_client = Chord_gRPC_Client(self.ip, self.port)
        
        # Variables protegidas por locks
        self.successor: Chord_gRPC_Client = None
        self.predecessor: Chord_gRPC_Client = None        
        self.predecessor_of_predecessor: Chord_gRPC_Client = None

        self.successor_lock = threading.Lock()
        self.predecessor_lock = threading.Lock()
        
        # Configuración del servidor gRPC
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        pb.add_ChordServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'[::]:{port}')
        self._server_thread = None
        self._stabilize_thread = None
        self._check_predecessor_thread = None
        self._logger_thread = None
        self._running = False

        self.contact_node = Chord_gRPC_Client("localhost", contact_port) if contact_port else None
        self.join(self.contact_node)        
        

    def start_server(self):
        self.server.start()
        logger.info(f"Nodo {self.port} iniciado con ID {self.id}")
        self._running = True
        self._server_thread = threading.Thread(target=self.server.wait_for_termination, daemon=True)
        self._server_thread.start()
        self._start_stabilizer()
        self._start_check_predecessor()
        self._start_logger()
        

    def stop_server(self):
        logger.info("Iniciando parada del servidor gRPC...")
        self._running = False
        # Detener todos los hilos primero
        for thread in [self._server_thread, self._stabilize_thread, 
                    self._check_predecessor_thread, self._logger_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=2)
        # Detener servidor gRPC
        if self.server:
            self.server.stop(0.5).wait()
            logger.info("Servidor gRPC detenido")



        # self._running = False
        # time.sleep(1)
        # self.server.stop(5)
        # if self._server_thread:
        #     self._server_thread.join()
        # if self._stabilize_thread:
        #     self._stabilize_thread.join()
        # if self._check_predecessor_thread:
        #     self._check_predecessor_thread.join()
        # if self._logger_thread:
        #     self._logger_thread.join()
        # logger.info(f"Nodo {self.port} detenido")    

    def _is_between(self, id: int, start: int, end: int) -> bool:
        if start < end:
            return start < id <= end
        return id > start or id <= end

    # Métodos del servidor gRPC
    def FindSuccessor(self, request: IdMessage, context) -> NodeMessage:
        try:
            target_id = request.id
            with self.successor_lock:
                current_successor = self.successor

            if current_successor.id == self.id and target_id != self.id:
                return self.grpc_client.node
            
            if self._is_between(target_id, self.id, current_successor.id):
                return current_successor.node
            else:
                new_successor = current_successor.find_successor(target_id)
                return new_successor.node
        except Exception as e:
            logger.error(f"Error en FindSuccessor: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return NodeMessage()
        
    def FindPredecessor(self, request: IdMessage, context) -> NodeMessage:
        id = request.id
        node = self.grpc_client
        succ = node.get_successor()        

        while not self._is_between(id, node.id, succ.id):
            node = succ
            succ = node.get_successor()
        return node.node

    def GetSuccessor(self, request: EmptyMessage, context) -> NodeMessage:
        with self.successor_lock:            
            return self.successor.node

    def GetPredecessor(self, request: EmptyMessage, context) -> NodeMessage:
        with self.predecessor_lock:
            if self.predecessor:
                return self.predecessor.node
            return NodeMessage()

    def Notify(self, request: NodeMessage, context) -> EmptyMessage:
        if self.id == request.id:
            return EmptyMessage()
        
        try:
            node = Chord_gRPC_Client(request.ip, request.port)
            with self.predecessor_lock:
                current_predecessor = self.predecessor
                if not current_predecessor:
                    self.predecessor = node
                    self.predecessor_of_predecessor = node.get_predecessor()
                    logger.info(f"Predecesor actualizado a {request.port}")
                elif node.ping():
                    if self._is_between(node.id, current_predecessor.id, self.id):
                        self.predecessor_of_predecessor = self.predecessor
                        self.predecessor = node
                        logger.info(f"Predecesor actualizado a {request.port}")                 
        except Exception as e:
            logger.error(f"Error en Notify: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))            

        return EmptyMessage()

    def NotAloneNotify(self, request: NodeMessage, context) -> EmptyMessage:
        node = Chord_gRPC_Client(request.ip, request.port)
        self.successor = node
        self.predecessor = node
        self.predecessor_of_predecessor = self.grpc_client
        return EmptyMessage()

    def ReverseNotify(self, request: NodeMessage, context) -> EmptyMessage:
        node = Chord_gRPC_Client(request.ip, request.port)
        with self.successor_lock:
            self.successor = node
        return EmptyMessage()
        
    def Ping(self, request: EmptyMessage, context) -> StatusResponseMessage:
        return StatusResponseMessage(ok=True)

    def join(self, node: Chord_gRPC_Client): 
        if node:
            if not node.ping():
                logger.error(f"Error en join, el nodo {node.port} no responde")
                return         
            
            with self.successor_lock:
                self.successor = node.find_successor(self.id)
            logger.info(f"Unido a la red. Sucesor: {self.successor.port}")            
            
            # Second node joins to chord ring
            if self.successor.get_successor().id == self.successor.id:
                with self.predecessor_lock:
                    self.predecessor = self.successor
                    self.predecessor_of_predecessor = self.grpc_client
                self.successor.not_alone_notify(self.grpc_client.node)         
        else:
            logging.info("Nueva red creada")
            self.successor = self.grpc_client            
            logger.info(f"Unido a la red. Sucesor: {self.successor.port}")          

    def _start_stabilizer(self):
        def stabilizer():
            while self._running:
                try:
                    self._stabilize()
                except Exception as e:
                    logger.error(f"Error en estabilización: {str(e)}")
                time.sleep(5)
        self._stabilize_thread = threading.Thread(target=stabilizer,daemon=True)
        self._stabilize_thread.start()

    def _stabilize(self):
        with self.successor_lock:
            successor = self.successor
        
        if not successor.ping() or successor.id == self.id:
            return

        try:
            pred_node = successor.get_predecessor()            
            if pred_node and self._is_between(pred_node.id, self.id, successor.id) and (pred_node.id != successor.id):                
                with self.successor_lock:
                    self.successor = pred_node
                    logger.info(f"Sucesor actualizado a {pred_node.port} durante estabilización")
            
            self.successor.notify(self.grpc_client.node)

            with self.predecessor_lock:
                current_predecessor = self.predecessor

            if current_predecessor and current_predecessor.ping():
                self.predecessor_of_predecessor = current_predecessor.get_predecessor()
            
        except Exception as e:
            logger.warning(f"Error estabilizando: {str(e)}")

    def _start_check_predecessor(self):
        def predecessor_checker():
            while self._running:
                try:
                    self._check_predecessor()
                except Exception as e:
                    logger.error(f"Error en chequeo de predecesor: {str(e)}")
                time.sleep(5)
        self._check_predecessor_thread = threading.Thread(target=predecessor_checker,daemon=True)
        self._check_predecessor_thread.start()

    def _check_predecessor(self):
        try:
            if self.predecessor and not self.predecessor.ping():
                logger.info(f"Predecesor {self.predecessor.port} no responde, eliminando")
                
                if self.predecessor_of_predecessor.ping():
                    self.predecessor = self.predecessor_of_predecessor        
                else:
                    self.predecessor = self.grpc_client.find_predecessor(self.predecessor_of_predecessor.id)
                
                logger.info(f"Predecesor actualizado a {self.predecessor.port} luego de haber caído")
                self.predecessor_of_predecessor = self.predecessor.get_predecessor()

                if self.id == self.predecessor.id:
                    self.successor = self.grpc_client
                    self.predecessor = None
                    self.predecessor_of_predecessor = None

                self.predecessor.reverse_notify(self.grpc_client.node)                  
        except Exception as e:
            self.predecessor = None
            self.successor = self.grpc_client
            logger.error(f"Error al comprobar predecesor: {str(e)}")          

    def _start_logger(self):
        def log_status():
            while self._running:
                with self.successor_lock:
                    succ = self.successor.port if self.successor else 'None'
                with self.predecessor_lock:
                    pred = self.predecessor.port if self.predecessor else 'None'
                logger.info(f"Estado actual - Sucesor: {succ}, Predecesor: {pred}")
                time.sleep(10)
        self._logger_thread = threading.Thread(target=log_status, daemon=True)
        self._logger_thread.start()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python chord_node.py <puerto> [puerto_contacto]")
        sys.exit(1)

    port = sys.argv[1]
    contact_port = sys.argv[2] if len(sys.argv) > 2 else None    

    try:
        node = ChordNode(port, contact_port)
        node.start_server()
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("\nInterrupción recibida")
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}")
    finally:
        node.stop_server()