from fastapi import FastAPI
import uvicorn

from bs4 import BeautifulSoup
import requests

import sys
import argparse


class ScraperServer():
    def __init__(self, ip, port) -> None:
        self.app = FastAPI()        
        self.configure_endpoints()
        self.run(ip,port)
        
    def configure_endpoints(self):  
        
        @self.app.post("/scrap")        
        def scrape(url: dict):
            print("Scrapeando " + str(url))
            url = url["url"]
            response = requests.get(url)    
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")    
            return {"status": "success", "data": str(soup)}
        
    def run(self, host, port):        
        uvicorn.run(self.app, host=host, port=port)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10000)    

    try:
        args = parser.parse_args()     
        server = ScraperServer(args.ip, args.port)
    except SystemExit as e:
        print(f"Error: {e}, argumentos recibidos: {sys.argv}")   


if __name__ == "__main__":
    main()



  