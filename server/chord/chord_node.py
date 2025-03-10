from concurrent import futures
from contextlib import contextmanager
import threading
import time
import grpc

from chord.autodiscovery_node import AutoDiscoveryNode
from chord.protos.chord_pb2 import IpMessage, IdMessage, StatusMessage, EmptyMessage
import chord.protos.chord_pb2_grpc as pb

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'   
)
logger = logging.getLogger(__name__)

# python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/chord.proto
   
from utils.const import RPC_PORT, M, FIX_FINGERS_INTERVAL, STABILIZE_INTERVAL, CHECK_PRED_INTERVAL, STATUS_INTERVAL
from utils.utils import hash_key, is_between

class NodeRef:
    def __init__(self, ip: str, port: int = RPC_PORT):
        self.id = hash_key(ip)
        self.ip = ip
        self.port = port


class ChordNode(pb.ChordServiceServicer, AutoDiscoveryNode):
    """
    Represents a node in a Chord distributed hash table (DHT) network
    """

    def __init__(self, ip):       
        

        self.id = hash_key(ip)
        self.ip = ip 
        self.ref = NodeRef(ip)

        self.fingers: list[NodeRef] = [self.ref] * M
        self.next = 0  # Finger table index to fix next
        
        # Variables protegidas por locks
        self.succ: NodeRef = None
        self.pred: NodeRef = None        
        self.pred2: NodeRef = None

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
        self._broadcast_thread = None
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

        self._broadcast_thread = threading.Thread(target=self._broadcast_listener, daemon=True)
        self._broadcast_thread.start()
        self._multicast_thread = threading.Thread(target=self._multicast_listener, daemon=True)
        self._multicast_thread.start()
        self._fix_fingers_thread = threading.Thread(target=self._fix_fingers, daemon=True)
        self._fix_fingers_thread.start()
        self._start_stabilizer()
        self._start_check_predecessor()
        self._start_logger()



    def join(self): 

        ip= self._discover_existing_nodes() if self._discover_existing_nodes() else None       
        
        
        if ip: 

            node = NodeRef(ip)           

            if not self.ping(node):
                logger.error(f"Error en join, el nodo {node.id} no responde")
                return         
            
            with self.succ_lock:                
                self.succ = self.find_succ(node,self.id)
            logger.info(f"Unido a la red. Sucesor: {self.succ.id}")            
            
            # Second node joins to chord ring
            if self.get_succ(self.succ).id == self.succ.id:
                with self.pred_lock:
                    self.pred = self.succ
                self.pred2 = self.ref
                self.not_alone(self.succ,self.ip)         
        else:
            logging.info(f"Nueva red creada.")
            self.succ = self.ref
        

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
                if self.ping(self.fingers[i]):
                    succ = self.find_succ(self.fingers[i],id)
                    return IpMessage(ip=succ.ip) 
        return IpMessage(ip=self.succ.ip)

        # id = request.id
        # node = self.ref
        # succ = self.get_succ(node)     

        # while not is_between(id, node.id, succ.id):
        #     node = succ
        #     succ = self.get_succ(node) 
        # return IpMessage(ip=succ.ip) 
    
        
        
    def FindPred(self, request: IdMessage, context) -> IpMessage:
        

        id = request.id
        node = self.ref
        succ = self.get_succ(node)     

        while not is_between(id, node.id, succ.id):
            node = succ
            succ = self.get_succ(node) 
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

        if self.ip == ip:
            return EmptyMessage()
        
        try:
            node = NodeRef(ip)
            with self.pred_lock:
                current_pred = self.pred
                if not current_pred:
                    self.pred = node
                    self.pred2 = self.get_pred(node)
                    logger.info(f"Predecesor actualizado a {node.id}")
                elif self.ping(node):
                    if is_between(node.id, current_pred.id, self.id):
                        self.pred2 = self.pred
                        self.pred = node
                        logger.info(f"Predecesor actualizado a {node.id}")                 
        except Exception as e:
            logger.error(f"Error en update_pred: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))            

        return EmptyMessage()

    def UpdateSucc(self, request: IpMessage, context) -> EmptyMessage:
        
        node = NodeRef(request.ip)
        with self.succ_lock:
            self.succ = node
            logger.info(f"Sucesor actualizado a {node.id}") 
        return EmptyMessage()

    def NotAlone(self, request: IpMessage, context) -> EmptyMessage:
        
        node = NodeRef(request.ip)
        self.succ = node
        self.pred = node
        self.pred2 = self.ref
        return EmptyMessage()    
        
    def Ping(self, request: EmptyMessage, context) -> StatusMessage:
        """
        GRPC method to ping the node.

        Returns:
        StatusMessage: A StatusMessage indicating whether the node is alive.
        """
        return StatusMessage(ok=True)
    
    #============END GRPC SERVER METHODS============


    #============GRPC CLIENT METHODS============

    @contextmanager
    def _get_channel(self, server: NodeRef):        
        channel = grpc.insecure_channel(f'{server.ip}:{server.port}')
        try:
            yield channel
        finally:
            channel.close()

    def _remote_call(self, server: NodeRef, method, request):
        with self._get_channel(server) as channel:
            stub = pb.ChordServiceStub(channel)            
            return getattr(stub, method)(request)            

    def find_succ(self, server: NodeRef, id: int) -> NodeRef:        
        response = self._remote_call(server,'FindSucc',IdMessage(id=id))
        return NodeRef(response.ip)        

    def find_pred(self, server: NodeRef, id: int) -> NodeRef:
        response = self._remote_call(server,'FindPred',IdMessage(id=id))
        return NodeRef(response.ip)        

    def get_succ(self, server: NodeRef) -> NodeRef:           
        response = self._remote_call(server,'GetSucc',EmptyMessage())
        return NodeRef(response.ip)             

    def get_pred(self, server: NodeRef) -> NodeRef:        
        response = self._remote_call(server,'GetPred',EmptyMessage())
        return NodeRef(response.ip)                        
    
    def update_pred(self, server: NodeRef, ip: str) -> None:    
        response = self._remote_call(server,'UpdatePred', IpMessage(ip=ip)) 
    
    def update_succ(self, server: NodeRef, ip: str) -> None:        
        response = self._remote_call(server,'UpdateSucc', IpMessage(ip=ip))

    def not_alone(self, server: NodeRef, ip: str) -> None:       
        response = self._remote_call(server,'NotAlone', IpMessage(ip=ip))  

    def ping(self, server: NodeRef) -> bool:
        try:
            response = self._remote_call(server,'Ping',EmptyMessage())
            return response.ok    
        except grpc.RpcError as e:
            return False


    #============END GRPC CLIENT METHODS============

    
    #============STABILIZATION============            

    

    def _stabilize(self):
        """
        Stabilization method.

        This method is called periodically to ensure the finger table is up to date.
        It checks if the current successor is alive and if it is the correct one.
        If the successor is not the correct one, it updates the finger table.
        It also updates the predecessor of the successor.
        """
        
        with self.succ_lock:
            succ = self.succ
        
        if not self.ping(succ) or succ.id == self.id:
            return

        try:
            pred_node = self.get_pred(succ)            
            if pred_node and is_between(pred_node.id, self.id, succ.id) and (pred_node.id != succ.id):                
                with self.succ_lock:
                    self.succ = pred_node
                    logger.info(f"Sucesor actualizado a {pred_node.id} durante estabilización")
            
            self.update_pred(succ,self.ip)

            with self.pred_lock:
                current_pred = self.pred

            if current_pred and self.ping(current_pred):
                self.pred2 = self.get_pred(current_pred)
            
        except Exception as e:
            logger.warning(f"Error estabilizando: {str(e)}")
   

    def _fix_fingers(self):
        """
        Periodically updates the finger table by finding the successor of the id calculated by id + 2^i mod 2^M.

        This method is called in a separate thread to run in the background.
        """
        
        batch_size = 10
        while True:
            for _ in range(batch_size):
                try:
                    self.next += 1
                    if self.next >= M:
                        self.next = 0                    
                    self.fingers[self.next] = self.find_succ(self.ref,(self.id + 2 ** self.next) % (2 ** M))
                except Exception as e:                    
                    pass
            time.sleep(FIX_FINGERS_INTERVAL)
    

    def _check_predecessor(self):
        """
        Checks if the predecessor is alive and if not, updates it.

        This method is called periodically to ensure the predecessor is up to date.
        If the predecessor is not alive, it updates the predecessor to the predecessor of the predecessor.
        If the predecessor of the predecessor is not alive either, it sets the predecessor to the current node.
        It also updates the successor of the predecessor to the current node if it is not the same node.
        """
        
        try:
            if self.pred and not self.ping(self.pred):
                logger.info(f"Predecesor {self.pred.id} no responde, eliminando")
                
                if self.ping(self.pred2):
                    self.pred = self.pred2        
                else:                    
                    self.pred = self.find_pred(self.ref,self.pred2.id)
                
                logger.info(f"Predecesor actualizado a {self.pred.id} luego de haber caído")
                self.pred2 = self.get_pred(self.pred)

                if self.id == self.pred.id:
                    self.succ = self.ref
                    self.pred = None
                    self.pred2 = None
                
                if self.pred:
                    self.update_succ(self.pred,self.ip)                  
        except Exception as e:
            self.pred = None
            self.succ = self.ref
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
                time.sleep(STABILIZE_INTERVAL)
        self._stabilize_thread = threading.Thread(target=stabilizer,daemon=True)
        self._stabilize_thread.start()

    def _start_check_predecessor(self):
        def predecessor_checker():
            while self._running:
                try:
                    self._check_predecessor()
                except Exception as e:
                    logger.error(f"Error en chequeo de predecesor: {str(e)}")
                time.sleep(CHECK_PRED_INTERVAL)
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
                time.sleep(STATUS_INTERVAL)
        self._logger_thread = threading.Thread(target=log_status, daemon=True)
        self._logger_thread.start()

#============END INFINITE LOOPS============

