from concurrent import futures
import sys
import threading
import time
import grpc

from chord.multicast_node import MulticastNode
from utils.utils import hash_key, is_between

from chord.chord_client import Chord_gRPC_Client
from chord.protos.chord_pb2 import NodeMessage, IdMessage, StatusResponseMessage, EmptyMessage
import chord.protos.chord_pb2_grpc as pb

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

   
GRPC_PORT = 50051
M = 32

class ChordNode(pb.ChordServiceServicer, MulticastNode):
    def __init__(self, ip):
        self.port = GRPC_PORT
        self.id = hash_key(ip,M)
        self.ip = ip            

        self.grpc_client = Chord_gRPC_Client(self.ip, self.port)
        self.m = M 
        self.fingers: list[Chord_gRPC_Client] = [self.grpc_client] * self.m
        self.next = 0  # Finger table index to fix next
        
        # Variables protegidas por locks
        self.succ: Chord_gRPC_Client = None
        self.pred: Chord_gRPC_Client = None        
        self.pred2: Chord_gRPC_Client = None

        self.succ_lock = threading.Lock()
        self.pred_lock = threading.Lock()
        
        # Configuración del servidor gRPC
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        pb.add_ChordServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'[::]:{self.port}')
        self._server_thread = None
        self._stabilize_thread = None
        self._check_predecessor_thread = None
        self._logger_thread = None
        self._multicast_thread = None
        self._fix_fingers_thread = None
        self._running = False

        # Autodescubrimiento multicast
        addr = self._discover_existing_nodes()
        contact_node = Chord_gRPC_Client(addr[0],int(addr[1])) if addr else None
        self.join(contact_node) 
      
        

    def start_server(self):
        self.server.start()
        logger.info(f"Nodo {self.ip} iniciado con ID {self.id}")
        self._running = True
        self._server_thread = threading.Thread(target=self.server.wait_for_termination, daemon=True)
        self._server_thread.start()
        self._multicast_thread = threading.Thread(target=self._multicast_listener, daemon=True)
        self._multicast_thread.start()
        self._fix_fingers_thread = threading.Thread(target=self._fix_fingers, daemon=True)
        self._fix_fingers_thread.start()
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


    # Métodos del servidor gRPC
    def FindSuccessor(self, request: IdMessage, context) -> NodeMessage:       
        id = request.id
        
        if self.id == id:
            return self.grpc_client.node           
        
        if is_between(id, self.id, self.succ.id):
            return self.succ.node
        
        # Otherwise, find the closest preceding node in the finger table and ask it.
        for i in range(len(self.fingers) - 1, -1, -1):
            if self.fingers[i] and is_between(self.fingers[i].id, self.id, id):
                if self.fingers[i].ping():
                    return self.fingers[i].find_successor(id).node   
        return self.succ.node    
        
        
        
    def FindPredecessor(self, request: IdMessage, context) -> NodeMessage:
        id = request.id
        node = self.grpc_client
        succ = node.get_successor()        

        while not is_between(id, node.id, succ.id):
            node = succ
            succ = node.get_successor()
        return node.node

    def GetSuccessor(self, request: EmptyMessage, context) -> NodeMessage:
        with self.succ_lock:            
            return self.succ.node

    def GetPredecessor(self, request: EmptyMessage, context) -> NodeMessage:
        with self.pred_lock:
            if self.pred:
                return self.pred.node
            return NodeMessage()

    def Notify(self, request: NodeMessage, context) -> EmptyMessage:
        if self.id == request.id:
            return EmptyMessage()
        
        try:
            node = Chord_gRPC_Client(request.ip, request.port)
            with self.pred_lock:
                current_predecessor = self.pred
                if not current_predecessor:
                    self.pred = node
                    self.pred2 = node.get_predecessor()
                    logger.info(f"Predecesor actualizado a {request.id}")
                elif node.ping():
                    if is_between(node.id, current_predecessor.id, self.id):
                        self.pred2 = self.pred
                        self.pred = node
                        logger.info(f"Predecesor actualizado a {request.id}")                 
        except Exception as e:
            logger.error(f"Error en Notify: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))            

        return EmptyMessage()

    def NotAloneNotify(self, request: NodeMessage, context) -> EmptyMessage:
        node = Chord_gRPC_Client(request.ip, request.port)
        self.succ = node
        self.pred = node
        self.pred2 = self.grpc_client
        return EmptyMessage()

    def ReverseNotify(self, request: NodeMessage, context) -> EmptyMessage:
        node = Chord_gRPC_Client(request.ip, request.port)
        with self.succ_lock:
            self.succ = node
        return EmptyMessage()
        
    def Ping(self, request: EmptyMessage, context) -> StatusResponseMessage:
        return StatusResponseMessage(ok=True)

    def join(self, node: Chord_gRPC_Client): 
        if node:
            if not node.ping():
                logger.error(f"Error en join, el nodo {node.id} no responde")
                return         
            
            with self.succ_lock:
                self.succ = node.find_successor(self.id)
            logger.info(f"Unido a la red. Sucesor: {self.succ.id}")            
            
            # Second node joins to chord ring
            if self.succ.get_successor().id == self.succ.id:
                with self.pred_lock:
                    self.pred = self.succ
                    self.pred2 = self.grpc_client
                self.succ.not_alone_notify(self.grpc_client.node)         
        else:
            logging.info("Nueva red creada")
            self.succ = self.grpc_client            
            logger.info(f"Unido a la red. Sucesor: {self.succ.id}")          

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
        with self.succ_lock:
            successor = self.succ
        
        if not successor.ping() or successor.id == self.id:
            return

        try:
            pred_node = successor.get_predecessor()            
            if pred_node and is_between(pred_node.id, self.id, successor.id) and (pred_node.id != successor.id):                
                with self.succ_lock:
                    self.succ = pred_node
                    logger.info(f"Sucesor actualizado a {pred_node.id} durante estabilización")
            
            self.succ.notify(self.grpc_client.node)

            with self.pred_lock:
                current_predecessor = self.pred

            if current_predecessor and current_predecessor.ping():
                self.pred2 = current_predecessor.get_predecessor()
            
        except Exception as e:
            logger.warning(f"Error estabilizando: {str(e)}")

    #     # Fix fingers method to periodically update the finger table
    # def fix_fingers(self):
    #     batch_size = 10
    #     while True:
    #         for _ in range(batch_size):
    #             try:
    #                 self.next += 1
    #                 if self.next >= self.m:
    #                     self.next = 0
    #                 self.finger[self.next] = self.lookup((self.id + 2 ** self.next) % 2 ** self.m)
    #             except Exception as e:
    #                 # print(f"Error in fix_fingers: {e}")
    #                 pass
    #         time.sleep(5)

    def _fix_fingers(self):
        batch_size = 10
        while True:
            for _ in range(batch_size):
                try:
                    self.next += 1
                    if self.next >= self.m:
                        self.next = 0
                    self.fingers[self.next] = self.grpc_client.find_successor((self.id + 2 ** self.next) % (2 ** self.m))
                except Exception as e:
                    # print(f"Error in fix_fingers: {e}")
                    pass
            time.sleep(5)


    def _start_check_predecessor(self):
        def predecessor_checker():
            while self._running:
                try:
                    self._check_predecessor()
                except Exception as e:
                    logger.error(f"Error en chequeo de predecesor: {str(e)}")
                time.sleep(2)
        self._check_predecessor_thread = threading.Thread(target=predecessor_checker,daemon=True)
        self._check_predecessor_thread.start()

    def _check_predecessor(self):
        try:
            if self.pred and not self.pred.ping():
                logger.info(f"Predecesor {self.pred.id} no responde, eliminando")
                
                if self.pred2.ping():
                    self.pred = self.pred2        
                else:
                    self.pred = self.grpc_client.find_predecessor(self.pred2.id)
                
                logger.info(f"Predecesor actualizado a {self.pred.id} luego de haber caído")
                self.pred2 = self.pred.get_predecessor()

                if self.id == self.pred.id:
                    self.succ = self.grpc_client
                    self.pred = None
                    self.pred2 = None

                self.pred.reverse_notify(self.grpc_client.node)                  
        except Exception as e:
            self.pred = None
            self.succ = self.grpc_client
            logger.error(f"Error al comprobar predecesor: {str(e)}")          

    def _start_logger(self):
        def log_status():
            while self._running:
                with self.succ_lock:
                    succ = self.succ.id if self.succ else 'None'
                with self.pred_lock:
                    pred = self.pred.id if self.pred else 'None'
                logger.info(f"Estado actual - Sucesor: {succ}, Predecesor: {pred}")
                time.sleep(10)
        self._logger_thread = threading.Thread(target=log_status, daemon=True)
        self._logger_thread.start()

