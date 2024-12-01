FROM python:3.11-alpine

WORKDIR /app

COPY . /app/

RUN pip install -r requirements.txt --no-cache-dir

RUN chmod +x /app/routing.sh

ENTRYPOINT [ "/app/routing.sh" ]