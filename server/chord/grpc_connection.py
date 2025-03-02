from chord.protos.chord_pb2 import IpMessage, IdMessage, EmptyMessage
import chord.protos.chord_pb2_grpc as pb

from utils.utils import hash_key

from contextlib import contextmanager
import grpc

import logging

# python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/chord.proto

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


RPC_PORT = 50051
M = 32

class GRPCConnection:    
    def __init__(self, ip):       
        
        self.id: int = hash_key(ip,M)     
        self.ip: str = ip

    @contextmanager
    def _get_channel(self):        
        channel = grpc.insecure_channel(f'{self.ip}:{RPC_PORT}')
        try:
            yield channel
        finally:
            channel.close()

    def _remote_call(self, method, request):
        with self._get_channel() as channel:
            stub = pb.ChordServiceStub(channel)            
            return getattr(stub, method)(request)            

    def find_succ(self, id: int) -> str:        
        response = self._remote_call('FindSucc',IdMessage(id=id))
        return response.ip        

    def find_pred(self, id: int) -> str:
        response = self._remote_call('FindPred',IdMessage(id=id))
        return response.ip        

    def get_succ(self) -> 'GRPCConnection':           
        response = self._remote_call('GetSucc',EmptyMessage())
        return GRPCConnection(response.ip)             

    def get_pred(self) -> 'GRPCConnection':        
        response = self._remote_call('GetPred',EmptyMessage())
        return GRPCConnection(response.ip)                       
    
    def update_pred(self, ip: str) -> None:    
        response = self._remote_call('UpdatePred', IpMessage(ip=ip)) 
    
    def update_succ(self, ip: str) -> None:        
        response = self._remote_call('UpdateSucc', IpMessage(ip=ip))

    def not_alone(self, ip: str) -> None:       
        response = self._remote_call('NotAlone', IpMessage(ip=ip))  

    def ping(self) -> bool:
        try:
            response = self._remote_call('Ping',EmptyMessage())
            return response.ok    
        except grpc.RpcError as e:
            return False