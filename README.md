# DiScrapy
Distributed Systems Project: Scrapper

chmod +x setup_infra.sh && ./setup_infra.sh


docker run -it --rm -p 10000:10000  --name server -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network servers scraper_server --ip "0.0.0.0"

docker run -it --rm -p 11000:11000 --name client1 -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network  clients scraper_client --ip "0.0.0.0" --port 11000 --entry_addr "10.0.11.2:10000"

source "/media/lido98/HD Datos/CIBER/Variado/Python/VirtualEnvs/sd_env/bin/activate"