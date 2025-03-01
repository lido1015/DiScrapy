import socket
import struct

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT = 5000
DISCOVERY_TIMEOUT = 2  


class MulticastNode():
    
    def _discover_existing_nodes(self):
        """Envía multicast para descubrir nodos existentes"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(DISCOVERY_TIMEOUT)
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        try:
            # Envía solicitud de descubrimiento
            sock.sendto( b'DISCOVER', (MULTICAST_GROUP, MULTICAST_PORT))
            
            # Escucha respuestas
            sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, 
                           socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('0.0.0.0'))            
            
            data, _ = sock.recvfrom(1024)
            ip, port = data.decode().split(':')
            logger.info(f"Nodo descubierto: {ip}:{port}")
            return [ip, int(port)]
        except socket.timeout:
            logger.info("No se encontraron nodos existentes. Iniciando nueva red.")
            return None
        except Exception as e:
            logger.error(f"Error en descubrimiento: {str(e)}")
            return None
        finally:
            sock.close()


    def _multicast_listener(self):
        """Escucha solicitudes de descubrimiento y responde"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', MULTICAST_PORT))
        sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, 
                       socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('0.0.0.0'))
        
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                if data == b'DISCOVER':
                    # Responde con nuestra información
                    response = f"{self.ip}:{self.port}".encode()
                    sock.sendto(response, addr)
                    logger.info(f"Respondiendo a descubrimiento desde {addr}")
            except Exception as e:
                continue            
            finally:
                sock.close()


