import os
import sys
import argparse
import zipfile
import requests

import streamlit as st

class Client:
    def __init__(self, entry_addr):
        self.entry_addr = entry_addr   
        self.already_scraped_urls = read_local_storage()     
        self.setup_ui()

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
        if not url:
            st.error("Please enter a valid URL.")
            return
        
        if not url.endswith('/'):
            url += '/'

        folder = folder_name(url)
        
        if url not in self.already_scraped_urls:

            print(f"Sending to server: {url}")
            request_url = f"http://{self.entry_addr}/scrape/?url={url}"

            try:
                response = requests.post(request_url)
                if response.status_code == 200:
                                    
                    zip_path = folder + ".zip"
                    print("1" + zip_path)

                    with open(zip_path, 'wb') as file:
                        file.write(response.content)
                    
                    with zipfile.ZipFile(zip_path, 'r') as file_zip:
                        file_zip.extractall(folder)
                    os.remove(zip_path)

                    update_local_storage(url)
                    self.already_scraped_urls.append(url)                    

                else:
                    st.error(f"Error downloading: {response.status_code}")

            except Exception as e:
                st.error(f"An error occurred: {e}")

        else:
            print(f"The url {url} is already scraped.")

        print(folder)
        with open(f"{folder}/index.html", 'r', encoding='utf-8') as html_file:
            html_content = html_file.read()

        #st.success("File downloaded and extracted successfully!")
        st.code(html_content, language='html')


def read_local_storage() -> list[str]:
    list = []
    with open('REQUESTS/index.txt', 'r') as archivo:
        list = archivo.read().splitlines()
    return list

def update_local_storage(url: str) -> None:
    with open('REQUESTS/index.txt', 'a') as archivo:        
        archivo.write(url + '\n')  

def folder_name(url):
    url = url[:-1]
    return "REQUESTS/" + url.split("//")[-1].replace("/", "_")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry_addr", type=str)

    try:
        args = parser.parse_args()     
        client = Client(args.entry_addr)  
    except SystemExit as e:
        print(f"Error: {e}, argumentos recibidos: {sys.argv}")   


if __name__ == "__main__":
    main()

