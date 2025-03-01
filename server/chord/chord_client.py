from chord.protos.chord_pb2 import NodeMessage, IdMessage, EmptyMessage
import chord.protos.chord_pb2_grpc as pb

from utils.utils import hash_key

from contextlib import contextmanager
import grpc

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Chord_gRPC_Client :    
    def __init__(self, ip, port):        
        """
        Initializes a Chord_gRPC_Client instance.

        Args:
            ip (str): The IP address of the node.
            port (str): The port number of the node.

        Returns:
            None
        """
        self.id: int = hash_key(ip,32)     
        self.ip: str = ip
        self.port: int = port

        self.node: NodeMessage = NodeMessage(id=self.id, ip=ip, port=port)



    @contextmanager
    def _get_channel(self):
        """Context manager para crear y manejar canales gRPC."""
        channel = grpc.insecure_channel(f'{self.ip}:{self.port}')
        try:
            yield channel
        finally:
            channel.close()

    def _remote_call(self, method, request):
        """
        Realiza una llamada RPC remota a un m todo en el nodo referenciado.

        Args:
            method (str): El nombre del m todo a llamar.
            request: El mensaje de solicitud a enviar.

        Returns:
            El mensaje de respuesta del servidor.

        Raises:
            grpc.RpcError: Si ocurre un error en la llamada RPC.
        """

        with self._get_channel() as channel:
            stub = pb.ChordServiceStub(channel)            
            return getattr(stub, method)(request)
            

    def find_successor(self, target_id) -> 'Chord_gRPC_Client':
        """Encuentra el sucesor de un ID en el nodo referenciado."""
        try:
            response = self._remote_call('FindSuccessor',IdMessage(id=target_id))
            return Chord_gRPC_Client(response.ip, response.port)
        except grpc.RpcError as e:
            logger.error(f"Error al encontrar sucesor de {target_id} en nodo {self.port}: {str(e)}")
            raise


    def find_predecessor(self, target_id) -> 'Chord_gRPC_Client':
        """Encuentra el predecesor de un ID en el nodo referenciado."""
        try:
            response = self._remote_call('FindPredecessor',IdMessage(id=target_id))
            return Chord_gRPC_Client(response.ip, response.port)
        except grpc.RpcError as e:
            logger.error(f"Error al encontrar predecesor de {target_id} en nodo {self.port}: {str(e)}")
            raise

    def get_successor(self) -> 'Chord_gRPC_Client': 
        """Obtiene el sucesor del nodo referenciado."""
        try:
            response = self._remote_call('GetSuccessor',EmptyMessage())
            return Chord_gRPC_Client(response.ip, response.port) 
        except grpc.RpcError as e:
            logger.error(f"Error al encontrar sucesor del nodo {self.port}: {str(e)}")
            raise
        

    def get_predecessor(self) -> 'Chord_gRPC_Client':   
        """Obtiene el predecesor del nodo referenciado."""
        try:
            response = self._remote_call('GetPredecessor',EmptyMessage())
            return Chord_gRPC_Client(response.ip, response.port) 
        except grpc.RpcError as e:
            logger.error(f"Error al encontrar predecesor del nodo {self.port}: {str(e)}")
            raise
               
    
    def notify(self, notifying_node):
        """Notifica al nodo referenciado sobre un posible nuevo predecesor."""
        try:
            response = self._remote_call('Notify', notifying_node)
            return response     
        except grpc.RpcError as e:
            logger.error(f"Error al notificar a {self.port}: {str(e)}")
            raise

    def not_alone_notify(self, notifying_node):
        try:
            response = self._remote_call('NotAloneNotify', notifying_node)
            return response     
        except grpc.RpcError as e:
            logger.error(f"Error al notificar a {self.port}: {str(e)}")
            raise

    def reverse_notify(self, notifying_node):
        try:
            response = self._remote_call('ReverseNotify', notifying_node)
            return response     
        except grpc.RpcError as e:
            logger.error(f"Error al notificar a {self.port}: {str(e)}")
            raise

    def ping(self) -> bool:
        try:
            response = self._remote_call('Ping',EmptyMessage())
            return response.ok    
        except grpc.RpcError as e:
            return False