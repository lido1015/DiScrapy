import os
import sys
import zipfile
import requests
import socket
import shutil
import struct
import argparse
import atexit
from urllib.parse import quote

import streamlit as st

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT = 5000
DISCOVERY_TIMEOUT = 5 

class ClientNode:
    def __init__(self, server):
        self.server = server        
        self.ip = socket.gethostbyname(socket.gethostname())
        self.storage_dir = self.create_storage_dir()
        self.already_scraped_urls = set()    
        self.setup_ui()

        atexit.register(self.shutdown)

    
    def create_storage_dir(self):        
        storage_dir = os.path.join('REQUESTS', self.ip)
        os.makedirs(storage_dir, exist_ok=True)
        return storage_dir

    def setup_ui(self):
        # Set the page configuration
        st.set_page_config(page_title="DiScrapy", layout="centered")

        css_file_path = "styles.css"
        if os.path.exists(css_file_path):
            with open(css_file_path, "r") as f:
                css_content = f.read()
                st.markdown(f'<style>{css_content}</style>', unsafe_allow_html=True)
        else:
            st.error(f"CSS file not found at: {css_file_path}")
        

        # Title of the app
        st.title("Welcome to DiScrapy!")

        # Input for URL
        url = st.text_input("Introduce the URL","https://www.google.com/")

        if st.button("Scrape"):
            with st.spinner("Scraping..."):
                self.send_url(url)


    def send_url(self, url):

        url = quote(url, safe='/:')

        # ips = self.discover_servers()
        # ip = ips[0] if ips else None
        # if not ip:
        #     logger.info("No hay servidores disponibles")
        #     return

        if not url:
            st.error("Please enter a valid URL.")
            return
        
        # if not url.endswith('/'):
        #     url += '/'

        folder = self.folder_name(url)
        
        if url not in self.already_scraped_urls:

            logger.info(f"Sending to server {self.server}: {url}")
            request_url = f"http://{self.server}:8000/scrape?url={url}"

            try:
                response = requests.post(request_url, timeout=15)
                response.raise_for_status()
                                    
                zip_path = folder + ".zip"                    

                with open(zip_path, 'wb') as file:
                    file.write(response.content)
                
                with zipfile.ZipFile(zip_path, 'r') as file_zip:
                    file_zip.extractall(folder)
                os.remove(zip_path)

                self.update_local_storage(url)
                self.already_scraped_urls.add(url)                 

            except requests.exceptions.HTTPError as e:
                st.error(
                    "**¡Ups! Algo salió mal al procesar tu solicitud.**\n\n"
                    "Parece que hubo un problema al intentar acceder a la página web que ingresaste. Esto puede deberse a:\n\n"
                    "1. La dirección web no existe o está mal escrita.\n"                     
                    "2. Problemas de conexión a internet.\n"                    
                    "3. La página web no está disponible en este momento.\n\n"
                    "**¿Qué puedes hacer?**\n"
                    "- Verifica que la URL esté escrita correctamente.\n"
                    "- Intenta nuevamente más tarde.\n"
                    "- Si el problema persiste, contáctanos para que podamos ayudarte."
                )
                return
            
            except requests.exceptions.ConnectionError:
                st.error("No se pudo conectar al servidor. Verifique su conexión")
                return
            
            except requests.exceptions.Timeout:
                st.error("Tiempo de espera agotado. Intente nuevamente")
                return
            
            except requests.exceptions.RequestException as e:
                st.error(f"Error de comunicación: {str(e)}")
                return
            
            except Exception as e:
                st.error(f"Error inesperado: {str(e)}")
                return

        else:
            logger.info(f"The url {url} is already scraped.")

        
        with open(f"{folder}/index.html", 'r', encoding='utf-8') as html_file:
            html_content = html_file.read()

        #st.success("File downloaded and extracted successfully!")
        st.code(html_content, language='html')

    def discover_servers(self):
        """Envía multicast para descubrir nodos servidores existentes"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(DISCOVERY_TIMEOUT)
        ttl = struct.pack('b', 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        
        try:
            # Primero unirse al grupo multicast
            sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('0.0.0.0'))
            
            # Enviar solicitud de descubrimiento
            sock.sendto(b'DISCOVER', (MULTICAST_GROUP, MULTICAST_PORT))
            
            # Recibir múltiples respuestas
            ips = []
            while True:
                try:
                    data, _ = sock.recvfrom(1024)
                    ip = data.decode()
                    logger.info(f"Nodo descubierto: {ip}")
                    if ip not in ips:
                        ips.append(ip)
                except socket.timeout:
                    break  # Finalizar al terminar el timeout
            
            return ips if ips else None
            
        except Exception as e:
            logger.error(f"Error en descubrimiento: {str(e)}")
            return None
        finally:
            # Dejar el grupo multicast antes de cerrar
            sock.setsockopt(socket.SOL_IP, socket.IP_DROP_MEMBERSHIP,
                        socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('0.0.0.0'))
            sock.close()


  
    def shutdown(self):
        shutil.rmtree(self.storage_dir, ignore_errors=True)
        


    def read_local_storage(self) -> list[str]:
        list = []
        with open(f'{self.storage_dir}/index.txt', 'r') as archivo:
            list = archivo.read().splitlines()
        return list

    def update_local_storage(self, url: str) -> None:
        with open(f'{self.storage_dir}/index.txt', 'a') as archivo:        
            archivo.write(url + '\n')  

    def folder_name(self,url):
        url = url[:-1]
        return f"{self.storage_dir}/" + url.split("//")[-1].replace("/", "_")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=str, default="10.0.11.2")

    try:
        args = parser.parse_args()     
        client = ClientNode(args.server)  
    except SystemExit as e:
        print(f"Error: {e}, argumentos recibidos: {sys.argv}")   


if __name__ == "__main__":
    main()
