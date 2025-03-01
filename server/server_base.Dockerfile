FROM python:3.11-alpine

# Instala dependencias del sistema (necesario para grpcio)
RUN apk add --no-cache g++ linux-headers

COPY requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir



