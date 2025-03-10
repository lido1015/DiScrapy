import os
import sys
import time
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
MULTICAST_PORT = 10000
DISCOVERY_TIMEOUT = 5
SL_PORT = 8501

class ClientNode:
    def __init__(self):
                
        self.ip = socket.gethostbyname(socket.gethostname())
        self.storage_dir = self.create_storage_dir()
        self.already_scraped_urls = set()    

        if 'server' not in st.session_state:
            st.session_state.server = self.discover_servers()


        if 'token' not in st.session_state:
            st.session_state.token = None
        if 'show_register' not in st.session_state:
            st.session_state.show_register = False
            st.session_state.register_error = None
        if 'show_signin' not in st.session_state:
            st.session_state.show_signin = False
            st.session_state.signin_error = None

        self.setup_ui()

        atexit.register(self.shutdown)


    def setup_ui(self):
        st.set_page_config(page_title="DiScrapy", layout="centered")

        # Cargar CSS
        css_file_path = "styles.css"
        if os.path.exists(css_file_path):
            with open(css_file_path, "r") as f:
                st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
        else:
            st.error(f"CSS file not found at: {css_file_path}")

        with st.container():
            st.title("Welcome to DiScrapy!")
            
            # Botones principales
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Register", key="register_button"):
                    st.session_state.show_register = True
                    st.session_state.show_signin = False
            with col2:
                if st.button("Log In", key="signin_button"):
                    st.session_state.show_signin = True
                    st.session_state.show_register = False
            with col3:
                if st.button("Log Out", key="signout_button"):
                    st.session_state.token = None 
                    st.error("The user logged out.")

            # Formulario de Registro
            if st.session_state.show_register:
                with st.form("register_form"):
                    username = st.text_input("Username", key="reg_username")
                    password = st.text_input("Password", type="password", key="reg_password")
                    confirm_password = st.text_input("Confirm Password", type="password", key="reg_confirm_password")
                    submitted = st.form_submit_button("Submit Registration")
                    
                    if submitted:
                        success = self.register(username, password, confirm_password)
                        if success:
                            st.session_state.show_register = False
                            st.session_state.register_error = None
                        else:
                            st.session_state.show_register = True  # Mantiene visible
                    
                    # Mostrar errores
                    if st.session_state.register_error:
                        st.error(st.session_state.register_error)

            # Formulario de Inicio de Sesión
            if st.session_state.show_signin:
                with st.form("signin_form"):
                    username = st.text_input("Username", key="signin_username")
                    password = st.text_input("Password", type="password", key="signin_password")
                    submitted = st.form_submit_button("Submit Log In")
                    
                    if submitted:
                        success = self.login(username, password)
                        if success:
                            st.session_state.show_signin = False
                            st.session_state.signin_error = None
                        else:
                            st.session_state.show_signin = True
                    
                    # Mostrar errores
                    if st.session_state.signin_error:
                        st.error(st.session_state.signin_error)

            # Input de URL
            url = st.text_input("Introduce the URL", "https://www.google.com/")
            if st.button("Scrape"):
                with st.spinner("Scraping..."):
                    self.send_url(url)


    def register(self, username, password, confirm_password):

        st.session_state.server = self.discover_servers()
        
        if not username.strip():
            st.session_state.register_error = "Username is required"
            return False
        if len(password) < 6:
            st.session_state.register_error = "Password must be at least 6 characters"
            return False
        if password != confirm_password:
            st.session_state.register_error = "Passwords do not match"
            return False        
        
        st.session_state.server = self.discover_servers()
        
        request_url = f"http://{st.session_state.server}:8000/authenticate"
        try:            
            response = requests.post(request_url, json=[username,password])
            response.raise_for_status()
            st.session_state.token = response.json()["access_token"]   
            st.success(f"✅ User {username} registered successfully!")
            return True
        except:
            st.session_state.register_error = f"User {username} already exist."
            return False
            


    def login(self, username, password):
        # Lógica de validación real
        if not username.strip() or not password.strip():
            st.session_state.signin_error = "Username and password are required"
            return False
        
        st.session_state.server = self.discover_servers()
        
        request_url = f"http://{st.session_state.server}:8000/login"        
        try:            
            response = requests.post(request_url, json=[username,password])
            response.raise_for_status()
            st.session_state.token = response.json()["access_token"]   
            st.success(f"✅ User {username} logged in successfully!")
            return True
        except requests.HTTPError:            
            st.session_state.signin_error = response.json()["detail"]
            return False


    def create_storage_dir(self):        
        storage_dir = os.path.join('REQUESTS', self.ip)
        os.makedirs(storage_dir, exist_ok=True)
        return storage_dir

        

    def send_url(self, url):

        url = quote(url, safe='/:')

        
        # ip = ips[0] if ips else None
        # if not ip:
        #     logger.info("No hay servidores disponibles")
        #     return

        if not url:
            st.error("Please enter a valid URL.")
            return

        folder = self.folder_name(url)        
        
        if url not in self.already_scraped_urls:

            st.session_state.server = self.discover_servers()

            logger.info(f"Sending to server {st.session_state.server}: {url}")

            request_url = f"http://{st.session_state.server}:8000/scrape?url={url}"
           
      
            headers = {
                "Authorization": f"Bearer {st.session_state.token}"              
            }

            try:                
                response = requests.post(
                    request_url,                        
                    headers=headers,
                    timeout=15
                )
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
                if e.response.status_code == 401:
                    st.error("Invalid credentials. Please log in.")
                else:
                    st.error(
                        "**Oops! Something went wrong while processing your request.**\n\n"
                        "It seems there was an issue trying to access the website you entered. This could be due to:\n\n"
                        "1. The web address does not exist or is misspelled.\n"
                        "2. Internet connectivity issues.\n"
                        "3. The website is temporarily unavailable.\n\n"
                        "**What can you do?**\n"
                        "- Check that the URL is correctly spelled.\n"
                        "- Try again later.\n"
                        "- If the problem persists, contact us for assistance."
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

    # def discover_servers(self):
    #     """Envía multicast para descubrir nodos servidores existentes"""
    #     sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #     sock.settimeout(DISCOVERY_TIMEOUT)
    #     ttl = struct.pack('b', 1)
    #     sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        
    #     try:
    #         # Primero unirse al grupo multicast
    #         sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
    #                     socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('0.0.0.0'))
            
    #         # Enviar solicitud de descubrimiento
    #         sock.sendto(b'DISCOVER', (MULTICAST_GROUP, MULTICAST_PORT))
            
    #         # Recibir múltiples respuestas
    #         ips = []
    #         while True:
    #             try:
    #                 data, _ = sock.recvfrom(1024)
    #                 ip = data.decode()
    #                 logger.info(f"Nodo descubierto: {ip}")
    #                 if ip not in ips:
    #                     ips.append(ip)
    #             except socket.timeout:
    #                 break  # Finalizar al terminar el timeout
            
    #         return ips if ips else None
            
    #     except Exception as e:
    #         logger.error(f"Error en descubrimiento: {str(e)}")
    #         return None
    #     finally:
    #         # Dejar el grupo multicast antes de cerrar
    #         sock.setsockopt(socket.SOL_IP, socket.IP_DROP_MEMBERSHIP,
    #                     socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('0.0.0.0'))
    #         sock.close()

    def discover_servers(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(DISCOVERY_TIMEOUT)  # Esperar 5 segundos por una respuesta


        # Unirse al grupo multicast
        group = socket.inet_aton(MULTICAST_GROUP)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.bind(("",MULTICAST_PORT))
        # Enviar solicitud de descubrimiento
        discover_message = b"DISCOVER"
        sock.sendto(discover_message, (MULTICAST_GROUP, MULTICAST_PORT))

        # Esperar respuesta de un nodo
        try:
            logger.info("Esperando respuesta de un nodo...")
            while True:
                data, addr = sock.recvfrom(1024)
                if data == b"DISCOVER":
                    continue
                node_ip = data.decode()  # La IP del nodo activo
                logger.info(f"Nodo descubierto: {node_ip}")
                break
            return node_ip
        except socket.timeout:
            print("No se recibió respuesta de ningún nodo.")
            return None
        finally:
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





if __name__ == "__main__":
    client = ClientNode() 


# def discover_servers(timeout: int = 3) -> list:
#     """
#     Sends a multicast request to discover servers and waits for responses.

#     :param timeout: Maximum time (in seconds) to wait for responses.
#     :return: List of discovered server IPs.
#     """
#     MCAST_GRP = "224.0.0.1"
#     MCAST_PORT = 10003
#     MESSAGE = "DISCOVER_SERVER"
#     BUFFER_SIZE = 1024

#     # Crear socket UDP para enviar y recibir
#     sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
#     sock.settimeout(timeout)

#     # Configurar TTL del paquete multicast
#     ttl = struct.pack("b", 1)
#     sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

#     # Enviar la petición multicast
#     try:
#         sock.sendto(MESSAGE.encode(), (MCAST_GRP, MCAST_PORT))
#     except Exception as e:
#         print(f"Error enviando el mensaje multicast: {e}")
#         return []

#     servers = []
#     start_time = time.time()
#     while True:
#         try:
#             data, addr = sock.recvfrom(BUFFER_SIZE)
#             server_ip = data.decode().strip()
#             servers.append(server_ip)
#             print(f"Servidor descubierto: {server_ip} (respuesta desde {addr})")
#         except socket.timeout:
#             break
#         except Exception as e:
#             print(f"Error recibiendo datos: {e}")
#             break
#         if time.time() - start_time > timeout:
#             break

#     sock.close()
#     return servers
