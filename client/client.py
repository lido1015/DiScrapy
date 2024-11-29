from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

import uvicorn
import requests

import sys
import argparse


class Client():
    def __init__(self, ip, port, entry_addr) -> None:        
        self.entry_addr = entry_addr

        self.app = FastAPI()
        self.templates = Jinja2Templates(directory="templates") 
        self.configure_endpoints()
        self.run(ip,port)
        
    def configure_endpoints(self):
        @self.app.get("/", response_class=HTMLResponse)
        def root(request: Request):               
            return self.templates.TemplateResponse("index.html",{"request":request})
        
        @self.app.post("/send_url")
        def send_url(url: dict):
            print("Enviando al servidor " + str(url))        
            request_url = f"http://{self.entry_addr}/scrap"
            try:
                response = requests.post(request_url, json=url)
                response.raise_for_status()
                return response.json()
            except Exception:
                return None   
       
        
    def run(self, ip, port):        
        uvicorn.run(self.app, host=ip, port=port)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--entry_addr", type=str, default="127.0.0.1:10000")

    try:
        args = parser.parse_args()     
        client = Client(args.ip, args.port, args.entry_addr)  
    except SystemExit as e:
        print(f"Error: {e}, argumentos recibidos: {sys.argv}")   


if __name__ == "__main__":
    main()



   


