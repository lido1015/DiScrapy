from concurrent import futures
import threading
import time
import grpc

from chord.multicast_node import MulticastNode
from utils.utils import hash_key, is_between

from chord.grpc_connection import GRPCConnection
from chord.protos.chord_pb2 import IpMessage, IdMessage, StatusMessage, EmptyMessage
import chord.protos.chord_pb2_grpc as pb

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

   
RPC_PORT = 50051
M = 32

class ChordNode(pb.ChordServiceServicer, MulticastNode):
    def __init__(self, ip):      
        
        
        self.id = hash_key(ip,M)
        self.ip = ip            

        self.conn = GRPCConnection(self.ip)
        
        self.fingers: list[GRPCConnection] = [self.conn] * M
        self.next = 0  # Finger table index to fix next
        
        # Variables protegidas por locks
        self.succ: GRPCConnection = None
        self.pred: GRPCConnection = None        
        self.pred2: GRPCConnection = None

        self.succ_lock = threading.Lock()
        self.pred_lock = threading.Lock()
        
        # Configuración del servidor gRPC
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        pb.add_ChordServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'[::]:{RPC_PORT}')
        self._server_thread = None
        self._stabilize_thread = None
        self._check_predecessor_thread = None
        self._logger_thread = None
        self._multicast_thread = None
        self._fix_fingers_thread = None
        self._running = False

        

        
    #============START AND JOIN============
        

    def start_server(self):

        self.server.start()
        logger.info(f"Nodo {self.ip} iniciado con ID {self.id}")
                
        self.join()

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

    def join(self): 

        # Autodescubrimiento multicast
        ip = self._discover_existing_nodes()

        if ip:

            node = GRPCConnection(ip)

            if not node.ping():
                logger.error(f"Error en join, el nodo {node.id} no responde")
                return         
            
            with self.succ_lock:
                self.succ = GRPCConnection(node.find_succ(self.id))
            logger.info(f"Unido a la red. Sucesor: {self.succ.id}")            
            
            # Second node joins to chord ring
            if self.succ.get_succ().id == self.succ.id:
                with self.pred_lock:
                    self.pred = self.succ
                self.pred2 = self.conn
                self.succ.not_alone(self.ip)         
        else:
            logging.info(f"Nueva red creada.")
            self.succ = self.conn  
        

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


    #============END START AND JOIN============      


    #============GRPC SERVER METHODS============

    def FindSucc(self, request: IdMessage, context) -> IpMessage:       
        id = request.id
        
        if self.id == id:
            return IpMessage(ip=self.ip)           
    
        if is_between(id, self.id, self.succ.id):
            return IpMessage(ip=self.succ.ip)
        
        # Otherwise, find the closest preceding node in the finger table and ask it.
        for i in range(len(self.fingers) - 1, -1, -1):
            if self.fingers[i] and is_between(self.fingers[i].id, self.id, id):
                if self.fingers[i].ping():
                    return IpMessage(ip=self.fingers[i].find_succ(id)) 
        return IpMessage(ip=self.succ.ip)       
        
        
    def FindPred(self, request: IdMessage, context) -> IpMessage:
        id = request.id
        node = self.conn
        succ = node.get_succ()       

        while not is_between(id, node.id, succ.id):
            node = succ
            succ = node.get_succ()
        return IpMessage(ip=node.ip)

    def GetSucc(self, request: EmptyMessage, context) -> IpMessage:
        with self.succ_lock:            
            return IpMessage(ip=self.succ.ip)

    def GetPred(self, request: EmptyMessage, context) -> IpMessage:
        with self.pred_lock:
            if self.pred:
                return IpMessage(ip=self.pred.ip)
            return IpMessage()

    def UpdatePred(self, request: IpMessage, context) -> EmptyMessage:
        ip = request.ip
        id = hash_key(ip,M)

        if self.id == id:
            return EmptyMessage()
        
        try:
            node = GRPCConnection(ip)
            with self.pred_lock:
                current_pred = self.pred
                if not current_pred:
                    self.pred = node
                    self.pred2 = node.get_pred()
                    logger.info(f"Predecesor actualizado a {id}")
                elif node.ping():
                    if is_between(node.id, current_pred.id, self.id):
                        self.pred2 = self.pred
                        self.pred = node
                        logger.info(f"Predecesor actualizado a {id}")                 
        except Exception as e:
            logger.error(f"Error en update_pred: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))            

        return EmptyMessage()

    def UpdateSucc(self, request: IpMessage, context) -> EmptyMessage:
        node = GRPCConnection(request.ip)
        with self.succ_lock:
            self.succ = node
            logger.info(f"Sucesor actualizado a {node.id}") 
        return EmptyMessage()

    def NotAlone(self, request: IpMessage, context) -> EmptyMessage:
        node = GRPCConnection(request.ip)
        self.succ = node
        self.pred = node
        self.pred2 = self.conn
        return EmptyMessage()    
        
    def Ping(self, request: EmptyMessage, context) -> StatusMessage:
        return StatusMessage(ok=True)
    
    #============END GRPC SERVER METHODS============

    
    #============STABILIZATION============            

    

    def _stabilize(self):
        with self.succ_lock:
            succ = self.succ
        
        if not succ.ping() or succ.id == self.id:
            return

        try:
            pred_node = succ.get_pred()            
            if pred_node and is_between(pred_node.id, self.id, succ.id) and (pred_node.id != succ.id):                
                with self.succ_lock:
                    self.succ = pred_node
                    logger.info(f"Sucesor actualizado a {pred_node.id} durante estabilización")
            
            self.succ.update_pred(self.ip)

            with self.pred_lock:
                current_pred = self.pred

            if current_pred and current_pred.ping():
                self.pred2 = current_pred.get_pred()
            
        except Exception as e:
            logger.warning(f"Error estabilizando: {str(e)}")
   

    def _fix_fingers(self):
        batch_size = 10
        while True:
            for _ in range(batch_size):
                try:
                    self.next += 1
                    if self.next >= M:
                        self.next = 0
                    ip = self.conn.find_succ((self.id + 2 ** self.next) % (2 ** M))
                    self.fingers[self.next] = GRPCConnection(ip)
                except Exception as e:                    
                    pass
            time.sleep(5)


    

    def _check_predecessor(self):
        try:
            if self.pred and not self.pred.ping():
                logger.info(f"Predecesor {self.pred.id} no responde, eliminando")
                
                if self.pred2.ping():
                    self.pred = self.pred2        
                else:
                    ip = self.conn.find_pred(self.pred2.id)
                    self.pred = GRPCConnection(ip)
                
                logger.info(f"Predecesor actualizado a {self.pred.id} luego de haber caído")
                self.pred2 = self.pred.get_pred()

                if self.id == self.pred.id:
                    self.succ = self.conn
                    self.pred = None
                    self.pred2 = None
                
                if self.pred:
                    self.pred.update_succ(self.ip)                  
        except Exception as e:
            self.pred = None
            self.succ = self.conn
            logger.error(f"Error al comprobar predecesor: {str(e)}")  

    #============END STABILIZATION============  
          

    #============INFINITE LOOPS============

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

    #============END INFINITE LOOPS============

