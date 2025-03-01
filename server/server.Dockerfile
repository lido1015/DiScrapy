FROM server:base

WORKDIR /app

COPY . /app/

EXPOSE 8000 50051

RUN chmod +x /app/routing.sh

ENTRYPOINT [ "/app/routing.sh" ]

