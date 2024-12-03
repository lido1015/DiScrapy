# DiScrapy

Proyecto de la asignatura Sistemas Distribuidos, 
de la carrera Ciencia de la Computación en la Universidad
de La Habana

- **Tema:** Scraper (1era Entrega, versión centralizada)
- **Autor:** [Lidier Robaina Caraballo](https://github.com/lido1015)
- **Grupo:** C-411

---

## Descripción

DiScrapy es una aplicación cliente - servidor que consiste en un scraper web sencillo.

- **Cliente:** Envía al servidor la url insertada por el usuario, recibe un archivo .zip
que contiene la información escrapeada, y lo descomprime para poner su contenido a disposición
del usuario en una carpeta con el nombre de la url. La interfaz gráfica permite la inspección visual y descarga del archivo .html.

- **Servidor:** Recibe del cliente la url a escrapear y le envía un archivo .zip con los archivos .html, .css y .js del sitio
web correspondiente. Cada uno de estos archivos es almacenado localmente para responder a futuros pedidos de urls ya escrapeadas. El proceso de scraping se apoya en la biblioteca de python BeautifulSoup. 


---

## Ejecución

El único requerimiento para la ejecución del proyecto es tener docker instalado en la pc.

```bash
# Clonar el repositorio
git clone https://github.com/lido1015/DiScrapy

# Ir al directorio del proyecto
cd DiScrapy

# Construir las imágenes de docker y ejecutar los contenedores necesarios 
chmod +x setup_infra.sh && ./setup_infra.sh

# Ejecutar la aplicación servidor
docker run -it --rm -p 10000:10000 --name server -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network servers -v $(pwd):/server/app server --ip "0.0.0.0"

# Ejecutar la aplicación cliente
# Modificar el parámetro --port para ejecutar varios clientes.
# La interfaz de usuario estará disponible en el navegador local en la dirección "http://0.0.0.0:{port}/"
docker run -it --rm -p 11000:11000 --name client1 -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network  clients -v $(pwd):/client/app client --ip "0.0.0.0" --port 11000 --entry_addr "10.0.11.2:10000"

```



