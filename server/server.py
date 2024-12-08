from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn

import os
import sys
import argparse

from scraper import scrape


class ScraperServer():
    def __init__(self, ip, port) -> None:
        self.storage = read_storage()
        self.app = FastAPI()        
        self.configure_endpoints()        
        self.run(ip,port)
     
        
    def configure_endpoints(self):  
        
        @self.app.post("/scrape")        
        def scrape_request(url: str):
                      
            try:    
                if url not in self.storage:
                    print("Scraping " + url)  
                    scrape(url)                    
                    update_storage(url) 
                    self.storage.append(url)
                else:
                    print(f"The url {url} is already scraped.")

                zip_file = folder_name(url) + ".zip"
                path = os.path.join('storage/', zip_file)
                response = FileResponse(path=path,filename=zip_file,media_type='application/zip')             
                return response
            except Exception as e:                    
                return {"error": str(e)}  

      
    def run(self, host, port):        
        uvicorn.run(self.app, host=host, port=port)


def read_storage() -> list[str]:
    list = []
    with open('storage/index.txt', 'r') as archivo:
        list = archivo.read().splitlines()
    return list

def update_storage(url: str) -> None:
    with open('storage/index.txt', 'a') as archivo:        
        archivo.write(url + '\n')   

def folder_name(url):
    url = url[:-1]
    return url.split("//")[-1].replace("/", "_")  



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



  