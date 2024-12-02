FROM client:base

WORKDIR /app

COPY . /app/

RUN chmod +x /app/routing.sh

ENTRYPOINT [ "/app/routing.sh" ]
