import socket
import struct
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from utils.const import MULTICAST_GROUP, MULTICAST_PORT, DISCOVERY_TIMEOUT, BROADCAST_PORT, BROADCAST_ADDRESS

class AutoDiscoveryNode():
    
    def _discover_existing_nodes(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1) #Permite broadcast

        sock.settimeout(DISCOVERY_TIMEOUT)  # Tiempo máximo para esperar una respuesta

        message = "DISCOVER_REQUEST"

        try:
            sock.sendto(message.encode('utf-8'), (BROADCAST_ADDRESS, BROADCAST_PORT))
            
            while True:
                try:
                    data, addr = sock.recvfrom(1024)
                    ip = data.decode('utf-8')                    

                    if ip == self.ip:
                        continue
                    logger.info(f"Nodo encontrado: {ip}")
                    return ip                        

                except socket.timeout:
                    print("No se encontraron servidores.")
                    return None  # No se encontró ningún servidor

        except Exception as e:
            print(f"Error durante el descubrimiento: {e}")
            return None
        finally:
            sock.close()

    def _broadcast_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permite reutilizar la dirección
        sock.bind(('', BROADCAST_PORT))

        while True:
            try:
                data, addr = sock.recvfrom(1024)
                message = data.decode('utf-8')

                if message == "DISCOVER_REQUEST":
                    response = f"{self.ip}".encode('utf-8')
                    sock.sendto(response, addr)
   
            except Exception as e:
                time.sleep(1)


    def _multicast_listener(self):
        """Escucha solicitudes de descubrimiento y responde"""
        # Crear socket multicast
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', MULTICAST_PORT))

        # Unirse al grupo multicast
        group = socket.inet_aton(MULTICAST_GROUP)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        while True:
            try:
                data, addr = sock.recvfrom(1024)
                if data == b'DISCOVER':
                    # Responde con nuestra información
                    response = f"{self.ip}".encode()
                    sock.sendto(response, (MULTICAST_GROUP,MULTICAST_PORT))
                    logger.info(f"Respondiendo a descubrimiento desde clientes")      
                    
            except Exception as e:                
                time.sleep(1)
      
